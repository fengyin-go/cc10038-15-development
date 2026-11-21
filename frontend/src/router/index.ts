import { createRouter, createWebHistory } from 'vue-router'

import Overview from '@/views/Overview.vue'
import QualityConsoleView from '@/views/QualityConsoleView.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    { path: '/quality-console', name: 'quality-console', component: QualityConsoleView },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
