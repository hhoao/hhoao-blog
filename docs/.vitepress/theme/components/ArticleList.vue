<template>
    <ClientOnly>
        <div class="mt-12">
            <div v-show="isShow(post)" v-for="post of posts" v-bind:key="post">
                <div @click="router.go(withBase(post.url))"
                    class="relative border-solid hover:text-gray-500 text-gray-600 hover:border-l-4 pt-1 transition-all duration-500 px-4 m-auto max-w-screen-md mt-14 hover:cursor-pointer">
                    <a class="text-2xl hover:text-balance  text-gray-600 font-bold  border-b-solid">
                        {{ post.frontmatter.title }}
                    </a>
                    <p class="my-2 text-sm">
                        <Icon class="mr-1" icon="uiw:date" />
                        <span>{{ dayjs(post.frontmatter.date).format("YYYY-MM-DD HH:mm:ss") }}</span>
                    </p>
                    <div v-html="truncateHtmlContent(post.html)"></div>
                    <template v-for="tag of post.frontmatter.tags" v-bind:key="tag">
                        <el-tag class="mt-2" @mouseleave="tagShow = false" type="info" @mouseover="tagShow = true"
                            v-if="tagShow">
                            {{ tag }}
                        </el-tag>
                    </template>
                </div>
            </div>
        </div>
    </ClientOnly>
</template>

<script setup>
import { data as posts } from '../utils/posts.data.js'
import { useRouter, withBase } from 'vitepress';
import DOMPurify from "dompurify"
import dayjs from "dayjs";
const router = useRouter();

function isShow(post) {
    return post.html && post.frontmatter.complete && post.frontmatter.title && post.frontmatter.date
}
function truncateHtmlContent(html, maxLength = 200) {
    const sanitizedHtml = DOMPurify.sanitize(html);
    const tempDiv = document.createElement("div");
    tempDiv.innerHTML = sanitizedHtml;
    const pElements = tempDiv.querySelectorAll("p");
    const paragraphs = Array.from(pElements).map((p) => p.innerText);
    const textContent = paragraphs.join(" ");
    const truncatedText = textContent.length > maxLength
        ? textContent.slice(0, maxLength) + "..."
        : textContent;
    return `<p>${truncatedText}</p>`;
}
</script>
