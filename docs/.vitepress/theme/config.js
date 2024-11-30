import { defineConfig } from "vitepress";

export default defineConfig({
  vite: {
    css: {
      preprocessorOptions: {
        css: {
          additionalData: '@import "./.vitepress/theme/style.css";',
        },
      },
    },
    plugins: {
      
    }
  },
});
