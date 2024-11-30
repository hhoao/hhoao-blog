/** @type {import('tailwindcss').Config} */
export default {
  content: [
    './docs/.vitepress/theme/**/*.{md,vue}',
    './docs/**/*.{md,vue,ts,js}'
  ],
  options: {
    safelist: ["html", "body"],
  },
};