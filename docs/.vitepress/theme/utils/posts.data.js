import { createContentLoader } from "vitepress";

export default createContentLoader("./blog/**/*.md", {
  includeSrc: true, // include raw markdown source?
  render: true, // include rendered full page HTML?
  excerpt: true, // include excerpt?
  transform(rawData) {
    return rawData
      .sort((a, b) => {
        return +new Date(b.frontmatter.date) - +new Date(a.frontmatter.date);
      })
      .map((page) => {
        let tags = [];
        tags.push(...page.url.split("/").slice(0, -1))
        if (page.frontmatter.tags && page.frontmatter.tags instanceof Array) {
          tags.push(...page.frontmatter.tags)
        }
        if (page.frontmatter.category && page.frontmatter.category instanceof Array) {
          tags.push(...page.frontmatter.category)
        }
        tags = new Set(tags);
        tags.delete("");
        tags.delete(null);
        page.frontmatter.tags = Array.from(tags);
        return {
          html: page.html,
          frontmatter: page.frontmatter,
          url: page.url,
        };
      });
  },
});
