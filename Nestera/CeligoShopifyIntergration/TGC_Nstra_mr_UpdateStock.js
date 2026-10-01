/**
 * Script Record : TGC_Nstra_mr_UpdateStock.js
 * Script ID     : customscript_tgc_nestera_mr_updatestock
 * Description   : Updates Kit/Package UK, US, EU, and Garden stock fields from the primary component’s 
 *                 positive on-hand quantities, using configured saved searches and subsidiary mappings. 
 *                 Sets unmatched regions to zero and logs invalid kits without updating them.
 * @Author       : LJA
 * Date          : 2026-09-29
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 */
define([
    'N/search',
    'N/record',
    'N/runtime',
    'N/log'
], (search, record, runtime, log) => {
        const PARAMS = {
            KIT_SEARCH: 'custscript_tgc_kit_search_id',
            STOCK_SEARCH: 'custscript_tgc_stock_search_id'
        };
        const REGIONS = [
            { name: 'UK', parameter: 'custscript_tgc_kit_sub_uk', field: 'custitem_tgc_kit_qtyavailuk' },
            { name: 'US', parameter: 'custscript_tgc_kit_sub_us', field: 'custitem_tgc_kit_qtyavailus' },
            { name: 'EU', parameter: 'custscript_tgc_kit_sub_eu', field: 'custitem_tgc_kit_qtyavaileu' },
            { name: 'Garden', parameter: 'custscript_tgc_kit_sub_garden', field: 'custitem_tgc_kit_qtyavailgarden' }
        ];

        const idValue = (value) => {
            if (Array.isArray(value)) {
                if (value.length !== 1) throw new Error('Expected one internal ID.');
                return idValue(value[0]);
            }
            if (value && typeof value === 'object') return idValue(value.value);
            const id = String(value == null ? '' : value).trim();
            if (!/^[1-9]\d*$/.test(id)) throw new Error('Invalid or missing internal ID: ' + id);
            return id;
        }

        const getConfig = () => {
            const script = runtime.getCurrentScript();
            const parameter = name => String(script.getParameter({ name }) || '').trim();
            const kitSearchId = parameter(PARAMS.KIT_SEARCH);
            if (!kitSearchId) throw new Error('Required parameter missing: ' + PARAMS.KIT_SEARCH);
            const seen = new Set();
            const regions = REGIONS.map(region => {
                const subsidiaryId = idValue(parameter(region.parameter));
                if (seen.has(subsidiaryId)) {
                    throw new Error('Subsidiary mappings must be distinct; duplicate for ' + region.name);
                }
                seen.add(subsidiaryId);
                return { ...region, subsidiaryId };
            });
            return {
                kitSearchId,
                stockSearchId: parameter(PARAMS.STOCK_SEARCH) || 'customsearch_tgc_ss_kitinvtrack',
                regions
            };
        }

        const andFilters = (savedSearch, extraFilters) => {
            const existing = savedSearch.filterExpression;
            savedSearch.filterExpression = existing && existing.length
                ? [existing, 'AND', extraFilters]
                : extraFilters;
        }

        const loadStockSearch = (config, componentId) => {
            const stockSearch = search.load({ id: config.stockSearchId });
            if (stockSearch.columns.some(column => column.summary)) {
                throw new Error('Inventory tracking search must have detail columns, not summary columns.');
            }
            const filters = [
                ['type', 'anyof', 'InvtPart'], 'AND',
                ['custitem_tgc_itm_kitprimcomp', 'is', 'T'], 'AND',
                ['locationquantityonhand', 'greaterthan', '0']
            ];
            if (componentId) filters.push('AND', ['internalid', 'anyof', componentId]);
            andFilters(stockSearch, filters);
            // In-memory column changes only. Preserve all saved search criteria.
            // Item + location provides stable paging for this detail search.
            stockSearch.columns = [
                search.createColumn({ name: 'internalid', sort: search.Sort.ASC }),
                search.createColumn({ name: 'inventorylocation', sort: search.Sort.ASC }),
                search.createColumn({ name: 'subsidiary', join: 'inventoryLocation' }),
                search.createColumn({ name: 'locationquantityonhand' })
            ];
            return stockSearch;
        }

        const getInputData = () => {
            const config = getConfig();
            // Check that the stock search is accessible before starting kit processing.
            loadStockSearch(config);
            const kitSearch = search.load({ id: config.kitSearchId });
            andFilters(kitSearch, [
                ['type', 'anyof', 'Kit'], 'AND',
                ['memberitem.custitem_tgc_itm_kitprimcomp', 'is', 'T']
            ]);
            // Add kit internal ID explicitly; itemid is a name, not a record ID.
            kitSearch.columns = [
                search.createColumn({ name: 'internalid', summary: search.Summary.GROUP, sort: search.Sort.ASC }),
                search.createColumn({ name: 'itemid', summary: search.Summary.GROUP }),
                search.createColumn({
                    name: 'internalid',
                    join: 'memberitem',
                    summary: search.Summary.GROUP,
                    sort: search.Sort.ASC
                })
            ];
            log.audit({ title: 'Kit stock configuration', details: config });
            return kitSearch;
        }

        const map = (context) => {
            const row = JSON.parse(context.value);
            const kitId = idValue(row.values['GROUP(internalid)']);
            // Read the joined internal ID, never the member item's display name.
            const acceptedKeys = [
                'group(internalid.memberitem)',
                'group(internalid).memberitem',
                'group(memberitem.internalid)'
            ];
            const componentKeys = Object.keys(row.values).filter(
                key => acceptedKeys.includes(key.toLowerCase())
            );
            if (componentKeys.length !== 1) {
                const keys = Object.keys(row.values);
                log.error({ title: 'Unexpected kit search columns', details: { kitId, keys } });
                throw new Error('Expected one Member Item Internal ID column. Returned keys: ' + keys.join(', '));
            }
            const componentKey = componentKeys[0];
            const componentId = idValue(row.values[componentKey]);
            context.write({ key: kitId, value: componentId });
        }

        // Joined location subsidiary search values can contain a name instead of an ID.
        const getLocationSubsidiaryId = (locationId, cache) => {
            if (!cache.has(locationId)) {
                const location = record.load({
                    type: record.Type.LOCATION,
                    id: locationId,
                    isDynamic: false
                });
                try {
                    cache.set(locationId, idValue(location.getValue({ fieldId: 'subsidiary' })));
                } catch (cause) {
                    throw new Error('Location ' + locationId +
                        ' must have one valid subsidiary internal ID. ' + cause.message);
                }
            }
            return cache.get(locationId);
        };

        const reduce = (context) => {
            const config = getConfig();
            const kitId = idValue(context.key);
            const componentIds = [...new Set(context.values.map(idValue))];
            if (componentIds.length !== 1) {
                throw new Error('Kit ' + kitId + ' has multiple primary components: ' + componentIds.join(', ') + '. No fields updated.');
            }
            const componentId = componentIds[0];
            // Distinguish invalid/non-inventory components from legitimate zero-stock results.
            const component = search.lookupFields({
                type: search.Type.INVENTORY_ITEM,
                id: componentId,
                columns: ['custitem_tgc_itm_kitprimcomp']
            });
            if (component.custitem_tgc_itm_kitprimcomp !== true &&
                component.custitem_tgc_itm_kitprimcomp !== 'T') {
                throw new Error('Component ' + componentId + ' is no longer marked as primary.');
            }

            const values = {};
            const fieldBySubsidiary = {};
            config.regions.forEach(region => {
                values[region.field] = 0;
                fieldBySubsidiary[region.subsidiaryId] = region.field;
            });
            const stockSearch = loadStockSearch(config, componentId);
            const columns = stockSearch.columns;
            const pages = stockSearch.runPaged({ pageSize: 1000 });
            const locations = new Map();
            const locationSubsidiaries = new Map();
            const unmappedSubsidiaries = new Set();

            pages.pageRanges.forEach(range => {
                const page = pages.fetch({ index: range.index });
                page.data.forEach(result => {
                    const returnedItemId = idValue(result.getValue(columns[0]));
                    if (returnedItemId !== componentId) throw new Error('Unexpected component in inventory search.');
                    const locationId = idValue(result.getValue(columns[1]));
                    const subsidiaryId = getLocationSubsidiaryId(locationId, locationSubsidiaries);
                    const rawQuantity = result.getValue(columns[3]);
                    const quantity = Number(rawQuantity);
                    if (rawQuantity === '' || rawQuantity == null || !Number.isFinite(quantity) || quantity < 0) {
                        throw new Error('Invalid on-hand quantity at location ' + locationId);
                    }
                    // Prevent duplicated joined search rows from multiplying stock.
                    const existing = locations.get(locationId);
                    if (existing) {
                        if (existing.quantity !== quantity || existing.subsidiaryId !== subsidiaryId) {
                            throw new Error('Conflicting inventory rows for location ' + locationId + '. No fields updated.');
                        }
                        return;
                    }
                    locations.set(locationId, { subsidiaryId, quantity });
                    const field = fieldBySubsidiary[subsidiaryId];
                    if (field) values[field] += quantity;
                    else unmappedSubsidiaries.add(subsidiaryId);
                });
            });

            record.submitFields({
                type: record.Type.KIT_ITEM,
                id: kitId,
                values,
                options: { enableSourcing: false, ignoreMandatoryFields: false }
            });
            context.write({
                key: kitId,
                value: JSON.stringify({ componentId, values, unmappedSubsidiaries: [...unmappedSubsidiaries] })
            });
        }

        const summarize = (summary) => {
            let errors = 0;
            let updated = 0;
            if (summary.inputSummary.error) {
                errors++;
                log.error({ title: 'Input error', details: summary.inputSummary.error });
            }
            [['Map', summary.mapSummary], ['Reduce', summary.reduceSummary]].forEach(([stage, result]) => {
                result.errors.iterator().each((key, message) => {
                    errors++;
                    log.error({ title: stage + ' error: ' + key, details: message });
                    return true;
                });
            });
            summary.output.iterator().each((kitId, value) => {
                updated++;
                const details = JSON.parse(value);
                if (details.unmappedSubsidiaries.length) {
                    log.audit({ title: 'Unmapped subsidiaries excluded: kit ' + kitId, details: details.unmappedSubsidiaries });
                }
                log.debug({ title: 'Kit updated: ' + kitId, details });
                return true;
            });
            log.audit({
                title: errors ? 'Kit stock update completed with errors' : 'Kit stock update completed',
                details: { updated, errors, seconds: summary.seconds, usage: summary.usage, yields: summary.yields }
            });
        }

        return { getInputData, map, reduce, summarize };
    });
