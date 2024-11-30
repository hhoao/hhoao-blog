import DefaultTheme from "vitepress/theme";
import "./style/custom.css";
import ArticleList from "./components/ArticleList.vue";
import ElementPlus from 'element-plus'
import "./style/tailwind.css";
import { Icon } from '@iconify/vue';



export default {
  extends: DefaultTheme,
  enhanceApp({ app }) {
    app.component("ArticleList", ArticleList);
    // eslint-disable-next-line vue/multi-word-component-names
    app.component("Icon", Icon);
    app.use(ElementPlus, { size: 'small', zIndex: 3000 })
  },
};
