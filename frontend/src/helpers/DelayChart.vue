<template>
  <div class="w-full max-w-3xl mx-auto">
    <Bar :data="chartData" :options="chartOptions" />
  </div>
</template>

<script setup>
import { Bar } from 'vue-chartjs'
import {
  Chart as ChartJS,
  Title,
  Tooltip,
  Legend,
  BarElement,
  CategoryScale,
  LinearScale
} from 'chart.js'

// Enregistre les composants nécessaires
ChartJS.register(Title, Tooltip, Legend, BarElement, CategoryScale, LinearScale)

const props = defineProps({
  delays: { type: Array, required: true },
  labels: { type: Array, required: true },
  title: { type: String, required: true }
})

// Exemple : delays = [{ airline: 'AF', delay: 12 }, { airline: 'LH', delay: 8 }]

const chartData = {
  labels: props.labels,
  datasets: [
    {
      label: 'Retard moyen (min)',
      backgroundColor: '#426d6c',
      data: props.delays
    }
  ]
}

const chartOptions = {
  responsive: true,
  plugins: {
    legend: {
      position: 'bottom'
    },
    title: {
      display: true,
      text: props.title
    }
  }
}
</script>

<style>
canvas {
  background: #fff;
  box-shadow: 0 4px 12px rgba(0,0,0,0.1);
}
</style>