import { useEffect, useState } from 'react'
import {
  Activity, Boxes, Database, FileBarChart2, GitBranch, Layers3,
  LayoutDashboard, ListChecks, Menu, PanelLeftClose, Search,
  ShieldCheck, Upload, UsersRound, type LucideIcon,
} from 'lucide-react'
import { Button } from './components/ui/button'
import { DashboardPage } from './pages/DashboardPage'
import { WorkflowPage } from './pages/WorkflowPages'

type NavItem = { label: string; path: string; icon: LucideIcon }
type NavGroup = { label: string; items: NavItem[] }

const navGroups: NavGroup[] = [
  { label: 'Workspace', items: [{ label: 'Dashboard', path: '/', icon: LayoutDashboard }] },
  { label: 'Data operations', items: [
    { label: 'Data ingestion', path: '/ingestion', icon: Upload },
    { label: 'Duplicate explorer', path: '/duplicates', icon: Layers3 },
    { label: 'Review queue', path: '/reviews', icon: ListChecks },
    { label: 'Quarantine', path: '/quarantine', icon: ShieldCheck },
  ] },
  { label: 'Governance', items: [
    { label: 'Master records', path: '/masters', icon: UsersRound },
    { label: 'Data lineage', path: '/lineage', icon: GitBranch },
    { label: 'Pipeline runs', path: '/runs', icon: Activity },
    { label: 'Reports & analytics', path: '/reports', icon: FileBarChart2 },
  ] },
]

function pathFromLocation() {
  return window.location.pathname === '/' ? '/' : window.location.pathname.replace(/\/$/, '')
}

function App() {
  const [activePath, setActivePath] = useState(pathFromLocation)
  const [sidebarOpen, setSidebarOpen] = useState(false)
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false)
  const [searchQuery, setSearchQuery] = useState('')

  useEffect(() => {
    const handlePopState = () => setActivePath(pathFromLocation())
    window.addEventListener('popstate', handlePopState)
    return () => window.removeEventListener('popstate', handlePopState)
  }, [])

  function navigateTo(path: string) {
    const target = new URL(path, window.location.href)
    window.history.pushState({}, '', target)
    setActivePath(target.pathname === '/' ? '/' : target.pathname.replace(/\/$/, ''))
    setSidebarOpen(false)
  }

  function navigate(event: React.MouseEvent<HTMLAnchorElement>, path: string) {
    event.preventDefault()
    navigateTo(path)
  }

  const activeItem = navGroups.flatMap((group) => group.items).find((item) => item.path === activePath)

  return (
    <div className={`app-frame ${sidebarCollapsed ? 'app-frame-collapsed' : ''}`}>
      <button className={`mobile-scrim ${sidebarOpen ? 'mobile-scrim-visible' : ''}`} onClick={() => setSidebarOpen(false)} aria-label="Close navigation" />
      <aside className={`sidebar ${sidebarOpen ? 'sidebar-open' : ''}`}>
        <div className="brand-lockup">
          <a className="brand-home" href="/" onClick={(event) => navigate(event, '/')} aria-label="MergeMind AI dashboard">
            <span className="brand-mark"><Boxes size={20} strokeWidth={2.2} /></span>
            <span className="brand-name">MergeMind<span>AI</span></span>
          </a>
          <button type="button" className="sidebar-collapse" onClick={() => setSidebarCollapsed((value) => !value)} aria-label={sidebarCollapsed ? 'Expand sidebar' : 'Collapse sidebar'} title={sidebarCollapsed ? 'Expand sidebar' : 'Collapse sidebar'}><PanelLeftClose size={17} /></button>
        </div>
        <div className="workspace-switcher" aria-label="Current workspace">
          <span className="workspace-avatar"><Database size={15} /></span>
          <span className="workspace-copy"><strong>Local workspace</strong><small>MergeMind database</small></span>
        </div>
        <nav className="primary-nav" aria-label="Primary navigation">
          {navGroups.map((group) => (
            <div className="nav-group" key={group.label}>
              <div className="nav-group-label">{group.label}</div>
              {group.items.map(({ label, path, icon: Icon }) => (
                <a key={path} href={path} onClick={(event) => navigate(event, path)} className={`nav-link ${activePath === path ? 'nav-link-active' : ''}`} aria-current={activePath === path ? 'page' : undefined} title={sidebarCollapsed ? label : undefined}>
                  <Icon size={17} strokeWidth={1.8} />
                  <span>{label}</span>
                </a>
              ))}
            </div>
          ))}
        </nav>
        <div className="sidebar-bottom"><span>ENTITY RESOLUTION · DATA QUALITY</span></div>
      </aside>

      <div className="workspace">
        <header className="topbar">
          <Button className="mobile-menu" variant="ghost" size="icon" onClick={() => setSidebarOpen(true)} aria-label="Open navigation"><Menu size={19} /></Button>
          <div className="breadcrumb"><span>MergeMind AI</span><span className="breadcrumb-slash">/</span><strong>{activeItem?.label ?? 'Workspace'}</strong></div>
          <div className="topbar-actions">
            <label className="global-search"><Search size={16} /><input value={searchQuery} onChange={(event) => setSearchQuery(event.target.value)} placeholder="Search records and runs..." aria-label="Search records and runs" /><kbd>⌘ K</kbd></label>
          </div>
        </header>

        {activePath === '/'
          ? <DashboardPage searchQuery={searchQuery} onSearchChange={setSearchQuery} />
          : <WorkflowPage path={activePath} searchQuery={searchQuery} onNavigate={navigateTo} />}
      </div>
    </div>
  )
}

export default App
