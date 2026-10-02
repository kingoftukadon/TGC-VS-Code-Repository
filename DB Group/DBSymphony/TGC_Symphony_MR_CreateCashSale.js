/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 */
define(['N/log'], (log) => {
    const getInputData = () => [
        { name: 'Laptop', category: 'Electronics', quantity: 2 },
        { name: 'Mouse', category: 'Electronics', quantity: 5 },
        { name: 'Desk', category: 'Furniture', quantity: 3 },
        { name: 'Chair', category: 'Furniture', quantity: 4 }
    ];

    const map = (context) => {
        const item = JSON.parse(context.value);

        context.write({
            key: item.category,
            value: item.quantity
        });
    };

    const reduce = (context) => {

        log.debug({
            title: 'Reducing Category',
            details: { category: context.key, quantities: context.values }
        });

        const totalQuantity = context.values.reduce(
            (total, quantity) => total + Number(quantity),
            0
        );

        context.write({
            key: context.key,
            value: totalQuantity
        });
    };

    const summarize = (summary) => {
        if (summary.inputSummary.error) {
            log.error({
                title: 'Input Error',
                details: summary.inputSummary.error
            });
        }

        summary.mapSummary.errors.iterator().each((key, error) => {
            log.error({ title: `Map Error: ${key}`, details: error });
            return true;
        });

        summary.reduceSummary.errors.iterator().each((key, error) => {
            log.error({ title: `Reduce Error: ${key}`, details: error });
            return true;
        });

        summary.output.iterator().each((category, totalQuantity) => {
            log.audit({
                title: `Total: ${category}`,
                details: { totalQuantity: Number(totalQuantity) }
            });
            return true;
        });

        log.audit({
            title: 'Demo Complete',
            details: {
                usage: summary.usage,
                seconds: summary.seconds,
                yields: summary.yields
            }
        });
    };

    return { getInputData, map, reduce, summarize };
});
