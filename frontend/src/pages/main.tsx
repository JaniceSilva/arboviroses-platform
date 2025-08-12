// src/main.tsx — exemplo de rotas
import React from 'react'
import ReactDOM from 'react-dom/client'
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import Dashboard from './pages/Dashboard'




ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Navigate to="/diamantina" replace />} />
        <Route path="/diamantina" element={<Dashboard muni="Diamantina" />} />
        <Route path="/teofilo-otoni" element={<Dashboard muni="Teófilo Otoni" />} />
      </Routes>
    </BrowserRouter>
  </React.StrictMode>,
)
