import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App'
import { ImageInvitePage } from './ImageBatchRuntime'
import './redesign.css'
import './workspace.css'
import './console-layout.css'
import './vision-workbench.css'

const mlInvite = new URLSearchParams(location.hash.slice(1)).get('ml-invite')
ReactDOM.createRoot(document.getElementById('root')!).render(<React.StrictMode>{mlInvite ? <ImageInvitePage token={mlInvite}/> : <App/>}</React.StrictMode>)
