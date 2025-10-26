<template>
    <div class="pb-8">
    <div class="flex flex-wrap justify-center">
        <div @click.prevent="changePage(1)" class="px-3 py-2 cursor-pointer border font-bold"><</div>
        <div @click.prevent="changePage(nbr)" :class="[actual_page == nbr ? 'bg-green-950 text-white font-bold' : 'border']" class="px-3 py-2 cursor-pointer" v-for="(nbr, index) in pages_count">{{ nbr }}</div>
        <div @click.prevent="changePage(nbr_page)" class="px-3 py-2 cursor-pointer border font-bold">></div>
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
        nbr_page: Number,
        current_page: Number,
        type: String
    },
    data() {
        return {
        }
    },
    methods: {
        changePage(page){
            let callable = {
                page: page,
                type: this.type
            };
            this.$emit('changePage', callable);
        }
    },
    computed: {
      async average_retard() {
        //let data = await this.get_flights_future()
        //return data;
      },
      pages_count() {
        return this.nbr_page;
      },
      actual_page() {
        return this.current_page;
      }
    },
    mounted() {
    },
}
</script>
