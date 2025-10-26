<template>
    <div class="relative h-full min-h-screen  back-green-light">
        <div class="z-10 relative">
          <div class="header-green-dark text-white py-10">
            <div class="px-6 py-4 w-full font-bold text-5xl uppercase text-center">
                Mon avion sera-t-il en retard ?
            </div>
            <div class="w-full text-center">Prédiction des retards des vols au départ et à l'arrivée des aéroports</div>
            <div class="w-full text-center">(MongoDB version.)</div>
          </div>
          <div class="px-24 pb-16">
          <div class="py-8"><Statistics :countFlight="24" /></div>
          <div class="font-bold pb-2"><h2>Menu</h2></div>

            <div>
              <div class="flex flex-wrap pb-8">
                <div @click.prevent="selected_nav = nav" :class="[nav.name == selected_nav.name ? 'bg-white text-green-950 font-bold' : 'bg-slate-300 text-green-950']" :key="index" v-for="(nav, index) in nav_menu" class="px-3 py-2 mr-2 cursor-pointer shadow-lg">{{ nav.label }}</div>
              </div>
            </div>
            <div class=" px-2 border border-green-950">
              <div class="py-8">
                <div>{{ flight_count_past }} vols au total.</div>
                <div class="font-bold" v-if="selected_nav.name == 'future_flight'">Légendes: 
                  <span class="m-2 pl-5 rounded-full w-2 bg-green-600"></span> A l'heure, 
                  <span class="m-2 pl-5 rounded-full w-2 bg-yellow-500"></span> Léger retard,
                  <span class="m-2 pl-5 rounded-full w-2 bg-orange-500"></span> En retard,
                  <span class="m-2 pl-5 rounded-full w-2 bg-red-600"></span> Très en retard

                </div>
              </div>
              <div class="flex flex-wrap pb-8" v-if="selected_nav.name == nav_menu[2].name">
                <div class="w-1/2 py-4">
                  <div><Chart :delays="delay_by_airline.map(d => d.delay).slice(0, 5)" :title="'Retard par compagnie'" :labels="delay_by_airline.map(d => getAirportName(d.AirlineID)).slice(0, 5)" /></div>
                </div>
                <div class="w-1/2 py-4">
                  <div><Chart :delays="delay_by_dep_airport.map(d => d.delay).slice(0, 5)" :title="'Retard par aéroport de départ'" :labels="delay_by_dep_airport.map(d => getAirportName(d.airport_code)).slice(0, 5)" /></div>
                </div>
                <div class="w-1/2 py-4">
                  <div><Chart :delays="delay_by_arr_airport.map(d => d.delay).slice(0, 5)" :title="'Retard par aéroport d\'arrivée'" :labels="delay_by_arr_airport.map(d => getAirportName(d.airport_code)).slice(0, 5)" /></div>
                </div>
              </div>
              <div class=" " v-if="selected_nav.name == nav_menu[1].name">
                <div v-if="flights_past.length" class="pb-8 flex flex-wrap justify-center w-full">
                  <div class="table-header">
                    <div class="table-row"></div>
                    <div class="table-row">Départ</div>
                    <div class="table-row">Arrivée</div>
                    <div class="table-row">Départ prévu</div>
                    <div class="table-row">Arrivée prévue</div>
                    <div class="table-row">Départ réel</div>
                    <div class="table-row">Arrivée réelle</div>
                    <div class="table-row">Température (°C)</div>
                    <div class="table-row">Status</div>
                  </div>
                  <div class="w-full">
                    <div class="flex flex-wrap w-full border-b py-2" v-for="(f, index) in flights_past" :key="index">
                      <div class="table-row">{{ index+1 }}</div>
                      <div class="table-row">{{ getAirportName(f.departure_airport) }}</div>
                      <div class="table-row">{{ getAirportName(f.arrival_airport)  }}</div>
                      <div class="table-row">{{ f.departure_scheduled }}</div>
                      <div class="table-row">{{ f.arrival_scheduled }}</div>
                      <div class="table-row">{{ f.departure_actual }}</div>
                      <div class="table-row">{{ f.arrival_actual }}</div>
                      <div class="table-row">{{ f.temp_departure }} / {{ f.temp_arrival }}</div>
                      <div class="table-row">{{ f.status }}</div>
                    </div>
                  </div>
                </div>

                <div v-else class="text-gray-500 py-4">
                  Chargement des vols...
                </div>
                <Pagination @changePage="changeOffset" :current_page="current_page_past" :nbr_page="nbr_page_past" :type="'past'"  />
              </div>              
              <div v-if="selected_nav.name == nav_menu[0].name" class="">
                <div v-if="flights_future.length" class="pb-8 flex flex-wrap justify-center w-full">
                  <div class="table-header">
                    <div class="table-row"></div>
                    <div class="table-row">Départ</div>
                    <div class="table-row">Arrivée</div>
                    <div class="table-row">Départ prévu</div>
                    <div class="table-row">Arrivée prévue</div>
                    <div class="table-row">Température (°C)</div>
                    <div class="table-row">Arrivée prédite</div>
                    <div class="table-row">Retard prédit (min)</div>
                  </div>
                  <div class="w-full">
                    <div class="flex flex-wrap w-full border-b py-2" v-for="(f, index) in flights_future" :key="index">
                      <div class="table-row">{{ index+1 }}</div>
                      <div class="table-row">{{ getAirportName(f.departure_airport) }}</div>
                      <div class="table-row">{{ getAirportName(f.arrival_airport)  }}</div>
                      <div class="table-row">{{ f.departure_scheduled }}</div>
                      <div class="table-row">{{ f.arrival_scheduled }}</div>
                      <div class="table-row">{{ f.temp_departure }} / {{ f.temp_arrival }}</div>
                      <div class="table-row">{{ f.arrival_predicted }}</div>
                      <div v-if="compare_date(f.arrival_scheduled, f.arrival_predicted)" :class="[diffInMinutes(f.arrival_scheduled, f.arrival_predicted) > 60 ? ' text-red-600' : diffInMinutes(f.arrival_scheduled, f.arrival_predicted) < 60 && diffInMinutes(f.arrival_scheduled, f.arrival_predicted) > 15 ? 'text-orange-500' : 'text-yellow-600' ]" class="table-row font-bold">{{diffInMinutes(f.arrival_scheduled, f.arrival_predicted)}} min 
                        <span v-if="diffInMinutes(f.arrival_scheduled, f.arrival_predicted) > 60" class="ml-2 pl-5 rounded-full w-2 bg-red-600"></span> 
                        <span v-if="diffInMinutes(f.arrival_scheduled, f.arrival_predicted) < 60 && diffInMinutes(f.arrival_scheduled, f.arrival_predicted) > 15" class="ml-2 pl-5 rounded-full w-2 bg-orange-500"></span> 
                        <span v-if="diffInMinutes(f.arrival_scheduled, f.arrival_predicted) <= 15" class="ml-2 pl-5 rounded-full w-2 bg-yellow-500"></span> 
                      </div>
                      <div v-else-if="diffInMinutes(f.arrival_scheduled, f.arrival_predicted) == 0" class="table-row">Vol à l'heure </div>
                      <div v-else class="table-row text-green-600 font-bold"> - {{diffInMinutes(f.arrival_scheduled, f.arrival_predicted)}} min <span class="ml-2 pl-5 rounded-full w-2 bg-green-600"></span> </div>
                    </div>
                  </div>
                </div>

                <div v-else class="text-gray-500 py-4">
                  Chargement des vols futures...
                </div>
                <Pagination @changePage="changeOffset" :current_page="current_page_future" :nbr_page="nbr_page_future" :type="'future'" />
              </div>
            </div>
          </div>
        </div>
    </div>
</template>
<script>
import axios from "axios";
import { ref, onMounted } from "vue";
import { format, isBefore } from 'date-fns'
import Pagination from './helpers/Pagination.vue'
import Statistics from './helpers/Statistics.vue'
import Chart from './helpers/DelayChart.vue'
const flights = ref([]);
export default {
    components: {Pagination, Statistics, Chart},
    props: {
        canLogin: Boolean,
        canRegister: Boolean,
        laravelVersion: String,
        phpVersion: String,
    },
    data() {
        return {
          flights_past: [],
          flights_future: [],
          flight_count_past: 0,
          flight_count_future: 0,
          delay_by_airline: [],
          delay_by_arr_airport: [],
          delay_by_dep_airport: [],
          airlines: [],
          airports: [],
          countries: [],
          records_by_page: 25,
          current_page_past: 1,
          current_page_future: 1,
          nav_menu:[
            {
              name: "future_flight",
              label: "Prédictions",
              selected: 1
            },
            {
              name: "past_flight",
              label: "Vols passés",
              selected: 0
            },
            {
              name: "stats",
              label: "Statistiques",
              selected: 0
            }
          ],
          selected_nav: 
            {
              name: "future_flight",
              label: "Prédictions",
              selected: 1
            },
        }
    },
    methods: {
      getAirportName(airportCode){
          let airport = this.airports.filter(a => a.airport_code == airportCode)[0];
          if (airport) {
            return airport.airport_name;
          }else {
            return airportCode;
          }
      },
      getAirlineName(airlineCode){
          let airline = this.airlines.filter(a => a.airline_code == airlineCode)[0];
          if (airline) {
            return airline.airline_name;
          }else {
            return airlineCode;
          }
      },
      changeOffset(page){
        let offset = page.page
        this.current_page = page.page
        if (page.type == 'past') {
          this.get_flights(offset);
        } else {
          this.get_flights_future(offset);          
        }
      },
        setIndex(index){
            this.index = index;
        },
        unsetIndex(){
            this.index = null;
        },
        changeIndex(index){
            if (this.index === index) {
                this.unsetIndex();
            }else {
                this.setIndex(index);
            }
        },
        compare_date(dt1, dt2) {
          var date1 = new Date(dt1);
          var date2 = new Date(dt2);
          if (date1 == date2) {
            return 0
          }
          return date1 < date2
        },
        diffInMinutes(dt1, dt2) {
          const date1 = new Date(dt1);
          const date2 = new Date(dt2);
          var diffMs = 0

          // différence en millisecondes
          if (date2 > date1) {
            diffMs = date2 - date1;
          }else {
            diffMs = date1 - date2;
          }


          // conversion en minutes
          const diffMinutes = diffMs / 1000 / 60;

          return diffMinutes;
        },
      getUrl(type, offset) {
        if (type == 'past') {
          return this.$apiBaseUrl+'/flights-past?limit='+this.records_by_page+'&offset='+offset;
        } else {
          return this.$apiBaseUrl+'/flights-future?limit='+this.records_by_page+'&offset='+offset;          
        }

      },
      async get_delay_by_airline() {
        let url = this.$apiBaseUrl+'/flights-stats-airlines'
        try {
          const res = await axios.get(url);

          this.delay_by_airline = res.data.map(f => ({
            AirlineID: f.OperatingCarrier_AirlineID,
            delay: f["delay (min)"]
          }));

          console.log("Stats :", this.delay_by_airline.length);
        } catch (err) {
          console.error("Erreur chargement stats :", err);
        }
      },
      async get_delay_by_arr_airport() {
        let url = this.$apiBaseUrl+'/flights-stats-arrival-airport'
        try {
          const res = await axios.get(url);

          this.delay_by_arr_airport = res.data.map(f => ({
            airport_code: f.Arrival_AirportCode,
            delay: f["delay (min)"]
          }));

          console.log("Stats :", this.delay_by_arr_airport.length);
        } catch (err) {
          console.error("Erreur chargement stats :", err);
        }
      },
      async get_delay_by_dep_airport() {
        let url = this.$apiBaseUrl+'/flights-stats-departure-airport'
        try {
          const res = await axios.get(url);

          this.delay_by_dep_airport = res.data.map(f => ({
            airport_code: f.Departure_AirportCode,
            delay: f["delay (min)"]
          }));

          console.log("Stats :", this.delay_by_dep_airport.length);
        } catch (err) {
          console.error("Erreur chargement stats :", err);
        }
      },
      async get_flights(offset) {
        let url = this.$apiBaseUrl+'/flights-past?limit='+this.records_by_page+'&offset='+offset
        try {
          const res = await axios.get(url);

          this.flights_past = res.data.map(f => ({
            id: f._id,
            departure_airport: f.Departure?.AirportCode,
            arrival_airport: f.Arrival?.AirportCode,
            departure_scheduled: `${f.Departure?.Scheduled?.Date} ${f.Departure?.Scheduled?.Time}`,
            arrival_scheduled: `${f.Arrival?.Scheduled?.Date} ${f.Arrival?.Scheduled?.Time}`,
            departure_actual: `${f.Departure?.Actual?.Date} ${f.Departure?.Actual?.Time}`,
            arrival_actual: `${f.Arrival?.Actual?.Date} ${f.Arrival?.Actual?.Time}`,
            status: f.Status?.Description,
            temp_departure: f.Departure?.weather?.temperature_2m ?? null,
            temp_arrival: f.Arrival?.weather?.temperature_2m ?? null,
          }));

          console.log("Vols chargés dans le passé :", this.flights_past.length);
        } catch (err) {
          console.error("Erreur chargement vols :", err);
        }
      },
      async get_flights_future(offset) {
        let url = this.$apiBaseUrl+'/flights-future?limit='+this.records_by_page+'&offset='+offset
        try {
          const res = await axios.get(url);

          this.flights_future = res.data.map(f => ({
            id: f._id,
            departure_airport: f.Departure?.AirportCode,
            arrival_airport: f.Arrival?.AirportCode,
            departure_scheduled: `${f.Departure?.Scheduled?.Date} ${f.Departure?.Scheduled?.Time}`,
            arrival_scheduled: `${f.Arrival?.Scheduled?.Date} ${f.Arrival?.Scheduled?.Time}`,
            departure_actual: `${f.Departure?.Actual?.Date} ${f.Departure?.Actual?.Time}`,
            arrival_actual: `${f.Arrival?.Actual?.Date} ${f.Arrival?.Actual?.Time}`,
            arrival_predicted: `${format(f.Arrival_Predicted_Datetime, 'yyyy/MM/dd HH:mm')}`,
            status: f.Status?.Description,
            temp_departure: f.Departure?.weather?.temperature_2m ?? null,
            temp_arrival: f.Arrival?.weather?.temperature_2m ?? null,
          }));

          console.log("Vols chargés dans futur :", this.flights_future.length);
        } catch (err) {
          console.error("Erreur chargement vols :", err);
        }
      },
      async get_flight_count_past() {
        let url = this.$apiBaseUrl+"/flights-count-past";
        try {
          const res = await axios.get(url);

          this.flight_count_past = res.data;

          console.log("Nombre de vols :", this.flights_past.length);
        } catch (err) {
          console.error("Erreur nombre de vols :", err);
        }
      },
      async get_flight_count_future() {
        let url = this.$apiBaseUrl+"/flights-count-future";
        try {
          const res = await axios.get(url);

          this.flight_count_future = res.data;

          console.log("Nombre de vols :", this.flights_past.length);
        } catch (err) {
          console.error("Erreur nombre de vols :", err);
        }
      },
      async get_airports() {
        let url = this.$apiBaseUrl+"/airports";
        try {
          const res = await axios.get(url);

          this.airports = res.data.map(f => ({
            airport_id: f._id,
            airport_code: f.AirportCode,
            country_code: f.CountryCode,
            airport_name: f.Names?.Name?.[5]?.$ != '??' ? f.Names?.Name?.[5]?.$ : f.Names?.Name?.[0]?.$
          }));

          console.log("Nombre d'aéroports' :", this.airports.length);
        } catch (err) {
          console.error("Erreur nombre d'aéroports :", err);
        }
      },
      async get_airlines() {
        let url = this.$apiBaseUrl+"/airlines";
        try {
          const res = await axios.get(url);

          this.airlines = res.data.map(f => ({
            airline_id: f._id,
            airline_code: f.AirlineID,
            airline_name: f.Names?.Name?.$
          }));

          console.log("Nombre d'aéroports' :", this.airlines.length);
        } catch (err) {
          console.error("Erreur nombre d'aéroports :", err);
        }
      },
      async get_countries() {
        let url = this.$apiBaseUrl+"/countries";
        try {
          const res = await axios.get(url);

          this.countries = res.data.map(f => ({
            country_id: f._id,
            country_code: f.CountryCode,
            country_name: f.Names?.Name?.[5]?.$ != '??' ? f.Names?.Name?.[5]?.$ : f.Names?.Name?.[0]?.$
          }));

          console.log("Nombre de pays' :", this.countries.length);
        } catch (err) {
          console.error("Erreur nombre de pays :", err);
        }
      },
    },
    computed: {
      async average_retard() {
        //let data = await this.get_flights_future()
        //return data;
      },
      nbr_page_past() {
        let nbr = this.flight_count_past/this.records_by_page;
        return Math.ceil(nbr);
      },
      nbr_page_future() {
        let nbr = this.flight_count_future/this.records_by_page;
        return Math.ceil(nbr);
      },
    },
    mounted() {
      console.log(this.$apiBaseUrl);
      this.get_flight_count_past();
      this.get_flight_count_future();
      this.get_flights(0);
      this.get_flights_future(0);
      this.get_countries();
      this.get_airports();
      this.get_airlines();
      this.get_delay_by_airline();
      this.get_delay_by_arr_airport();
      this.get_delay_by_dep_airport();
    },
}
</script>
<style lang="css">
.header-green-dark {
  background-color: rgb(66, 109, 108);
}
.back-green-light {
  background-color: rgb(207, 209, 192);
}
</style>
