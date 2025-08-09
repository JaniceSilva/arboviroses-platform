import React from 'react'
import { Line } from 'react-chartjs-2'
import { Chart as ChartJS, CategoryScale, LinearScale, PointElement, LineElement, Title, Tooltip, Legend } from 'chart.js'
ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Title, Tooltip, Legend)
export default function TimeSeriesChart({data}){
  const labels = data.map(d=>new Date(d.date).toLocaleDateString())
  const cases = data.map(d=>d.cases)
  const temp = data.map(d=>d.temp)
  const chartData = { labels, datasets: [{label:'Casos', data:cases, tension:0.2},{label:'Temp', data:temp, tension:0.2, yAxisID:'y1'}] }
  const opts = { responsive:true, scales:{ y:{position:'left'}, y1:{position:'right', grid:{drawOnChartArea:false}} } }
  return <Line data={chartData} options={opts} />
}
