/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./src/**/*.{js,jsx,ts,tsx}",
    "./public/index.html",
  ],
  theme: {
    extend: {
      fontFamily: {
        inter: ['Inter', 'sans-serif'],
      },
      colors: {
        'gray-800': '#2D3748', // Darker gray for backgrounds
        'gray-900': '#1A202C', // Even darker for gradients
        'gray-700': '#4A5568', // Card backgrounds
        'gray-600': '#718096', // Borders, table headers
        'gray-500': '#A0AEC0', // Footer text
        'gray-400': '#CBD5E0', // Light text
        'blue-300': '#90CDF4', // Accent blue for titles
        'purple-600': '#805AD5', // Primary button/accent purple
        'green-400': '#68D391', // Status green
        'yellow-400': '#F6E05E', // Status yellow
        'red-400': '#FC8181', // Status red
      },
    },
  },
  plugins: [],
}
