// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

// https://astro.build/config
export default defineConfig({
	site: 'https://qwer-q.netlify.app',
	integrations: [
		starlight({
			title: 'QWER-Q',
			description: 'A typed, docker-first message queue built in Go.',
			social: [
				{
					icon: 'github',
					label: 'GitHub',
					href: 'https://github.com/jonas/qwer-q',
				},
			],
			customCss: ['./src/styles/custom.css'],
			sidebar: [
				{
					label: 'Getting Started',
					slug: 'getting-started',
				},
				{
					label: 'Concepts',
					slug: 'concepts',
				},
				{
					label: 'Reference',
					items: [
						{ label: 'CLI Reference', slug: 'reference/cli' },
						{ label: 'Protocol Reference', slug: 'reference/protocol' },
						{ label: 'Configuration', slug: 'reference/configuration' },
						{ label: 'API Reference', slug: 'reference/api' },
					],
				},
				{
					label: 'Deployment',
					slug: 'deployment',
				},
				{
					label: 'FAQ & Troubleshooting',
					slug: 'faq',
				},
			],
		}),
	],
});
