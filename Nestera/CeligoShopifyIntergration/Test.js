practice.js

const message = "Hello from the main branch!";
const originalPrice = 1000;
const discountPercent = 10;

function calculateTotal(price, discount) {
  return price - (price * discount / 100);
}

console.log(message);
console.log('added console message!');
console.log(`Discount: ${discountPercent}%`);
console.log(`Total: PHP ${calculateTotal(originalPrice, discountPercent).toFixed(2)}`);
