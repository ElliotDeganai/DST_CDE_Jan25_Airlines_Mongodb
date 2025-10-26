<template>
    <div class="relative text-green-950">
        <div>
            <div class=" font-bold">Chiffres clés (30 derniers jours)</div>
            <div v-if="stats">
                <div class="flex flex-wrap">
                    <div class="table-stats-row border text-center text-sm" v-for="hd in table_header">{{hd}}  
                        <span v-if="hd == 'Retards >= 60 min'" class="ml-2 pl-5 rounded-full w-2 bg-red-600"></span> 
                        <span v-if="hd == 'Retards 16 à 59 min'" class="ml-2 pl-5 rounded-full w-2 bg-orange-500"></span> 
                        <span v-if="hd == 'Retard <= 15 min'" class="ml-2 pl-5 rounded-full w-2 bg-yellow-400"></span> 
                    </div>
                </div>
                <div class="flex flex-wrap text-green-800 font-bold bg-white">
                    <div class="table-stats-row" >{{stats.cnt_retarded}}</div>  
                    <div class="table-stats-row" >{{getPct(stats.pct_retarded_sup_15)}} %</div>  
                    <div class="table-stats-row" >{{Math.round(stats.mean)}} min</div>  
                    <div class="table-stats-row" >{{stats.cnt_retarded-stats.cnt_retarded_sup_15}}</div>  
                    <div class="table-stats-row" >{{stats.cnt_retarded_sup_15}}</div>  
                    <div class="table-stats-row" >{{stats.cnt_retarded_sup_60}}</div>  
                    <div class="table-stats-row" >{{stats.max}} min</div>     
                    <div class="table-stats-row" >{{stats.min}} min</div>                    
                </div>
            </div>
            <div v-else>Chargement des statistiques...</div>
        </div>
    </div>
</template>
<script>
import axios from "axios";
import { ref, onMounted } from "vue";
import { format, isBefore } from 'date-fns'
const flights = ref([]);
export default {
    components: {},
    props: {
        canLogin: Boolean,
        canRegister: Boolean,
        laravelVersion: String,
        countFlight: Number,
    },
    data() {
        return {
          stats: [],
          table_header: ['Vols retardés', 'Taux de retard > 15 min', 'Retard moyen', 'Retard <= 15 min', 'Retards 16 à 59 min', 'Retards >= 60 min', 'Retard max', 'Retard min']
        }
    },
    methods: {
        setIndex(index){
            this.index = index;
        },
        unsetIndex(){
            this.index = null;
        },
        getPct(figure){
            let num = figure*100;
            return parseFloat(num.toFixed(2));
        },
      async get_stats() {
        let url = this.$apiBaseUrl+'/flights-stats';
        try {
          const res = await axios.get(url);

          this.stats = res.data.map(f => ({
            id: f._id,
            mean: f.mean,
            min: f.min,
            max: f.max,
            pct_retarded: f.pct_retarded,
            pct_retarded_sup_15: f.pct_retarded_sup_15,
            pct_retarded_sup_30: f.pct_retarded_sup_30,
            pct_retarded_sup_60: f.pct_retarded_sup_60,
            cnt_retarded: f.cnt_retarded,
            cnt_retarded_sup_15: f.cnt_retarded_sup_15,
            cnt_retarded_sup_30: f.cnt_retarded_sup_30,
            cnt_retarded_sup_60: f.cnt_retarded_sup_60
          }));
          this.stats = this.stats[0];

          console.log("Statistiques chargées :", this.stats.length);
        } catch (err) {
          console.error("Erreur chargement des statistiques :", err);
        }
      }
    },
    computed: {
      async average_retard() {
        //let data = await this.get_flights_future()
        //return data;
      },
    },
    mounted() {
      this.get_stats();
    },
}
</script>
<style lang="css">
.header-green-dark {
  background-color: rgb(66, 109, 108);
}
</style>
