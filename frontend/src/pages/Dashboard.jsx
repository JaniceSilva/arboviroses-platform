import React, {useEffect,useState} from 'react'
import axios from 'axios'
import TimeSeriesChart from '../components/TimeSeriesChart'

const API = import.meta.env.VITE_API_URL || 'http://localhost:8000/api'

export default function Dashboard(){
  const [city,setCity]=useState('teofilo_otoni')
  const [data,setData]=useState([])
  const [pred,setPred]=useState([])
  useEffect(()=>{ fetchData(city) },[city])
  async function fetchData(c){
    try{
      const r = await axios.get(`${API}/data/${c}`)
      setData(r.data.data||[])
      const p = await axios.post(`${API}/predict`, {city:c, last_weeks:12})
      setPred(p.data.prediction_weeks||[])
    }catch(e){ console.error(e) }
  }
  return (<div className='dashboard'>
    <div className='controls'><label>Selecione: <select value={city} onChange={e=>setCity(e.target.value)}><option value='teofilo_otoni'>Teófilo Otoni</option><option value='diamantina'>Diamantina</option></select></label></div>
    <div className='cards'><div className='card'><h3>Última</h3><p>{data.length?data[data.length-1].cases:'—'}</p></div><div className='card'><h3>Pred</h3><p>{pred.length?pred.join(', '):'—'}</p></div></div>
    <div className='charts'><TimeSeriesChart data={data} /></div>
  </div>)
}
