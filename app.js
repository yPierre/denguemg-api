const express = require('express');
const { MongoClient } = require("mongodb");
const { performance } = require("perf_hooks");

const app = express();
const port = process.env.PORT || 3000; // Render define a porta via process.env.PORT

const uri = process.env.MONGODB_URI || "mongodb+srv://jeandias1997:GWtDa5xFwnKaYhgG@cluster0.njfbl.mongodb.net/";
const client = new MongoClient(uri);

// Funções auxiliares do seu código original
async function getPreviousCityAccumulated(db, geocode, currentSE) {
    const stateCollection = db.collection("statev3");
    const previousData = await stateCollection.findOne(
        { "cities.geocode": geocode },
        { 
            sort: { SE: -1 },
            projection: { 
                _id: 0,
                SE: 1,
                cities: { $elemMatch: { geocode: geocode } }
            }
        }
    );

    if (!previousData) return 0;

    const currentYear = parseInt(currentSE.toString().substring(0, 4));
    const previousYear = parseInt(previousData.SE.toString().substring(0, 4));

    if (currentYear > previousYear) return 0;

    return previousData.cities[0]?.notif_accum_year || 0;
}

async function getLastEpidemiologicalWeek(db) {
    const stateCollection = db.collection("statev3");
    const stateData = await stateCollection.findOne({}, { sort: { SE: -1 } });

    if (!stateData) {
        throw new Error("Nenhuma informação epidemiológica encontrada no banco.");
    }

    const latestSE = stateData.SE.toString();
    const year = parseInt(latestSE.substring(0, 4));
    const week = parseInt(latestSE.substring(4, 6));

    let ew_start, ew_end, ey_start, ey_end;
    if (week < 52) {
        ew_start = week + 1;
        ew_end = week + 1;
        ey_start = year;
        ey_end = year;
    } else {
        ew_start = 1;
        ew_end = 1;
        ey_start = year + 1;
        ey_end = year + 1;
    }

    console.log(`📌 Última SE encontrada: ${latestSE} → Requisitando dados para SE: ${ey_start}${ew_start.toString().padStart(2, '0')}`);
    return { ew_start, ew_end, ey_start, ey_end };
}

async function fetchCitiesMG() {
    const urlIBGE = "https://servicodados.ibge.gov.br/api/v1/localidades/estados/31/municipios";
    try {
        console.log("Buscando lista de cidades de MG...");
        const response = await fetch(urlIBGE);
        if (!response.ok) throw new Error(`Erro na API do IBGE: ${response.status}`);
        const cities = await response.json();
        return cities.map(city => ({ geocode: city.id, name: city.nome }));
    } catch (error) {
        console.error("Erro ao obter cidades de MG:", error.message);
        return [];
    }
}

async function fetchDengueData(db, geocode, cityName, ew_start, ew_end, ey_start, ey_end) {
    const apiUrl = "https://info.dengue.mat.br/api/alertcity";
    const params = new URLSearchParams({
        geocode: geocode,
        disease: "dengue",
        format: "json",
        ew_start: ew_start,
        ew_end: ew_end,
        ey_start: ey_start,
        ey_end: ey_end,
    }).toString();

    try {
        console.log(`Buscando dados para ${cityName} (${geocode})...`);
        const response = await fetch(`${apiUrl}?${params}`, {
            method: "GET",
            headers: { "Content-Type": "application/json" },
        });

        if (!response.ok) {
            console.warn(`⚠️ API InfoDengue retornou erro para ${cityName} (${geocode}): ${response.status}`);
            return null;
        }

        const data = await response.json();
        const currentSE = `${ey_start}${ew_start.toString().padStart(2, '0')}`;
        const previousAccumulated = await getPreviousCityAccumulated(db, geocode, currentSE);
        return data.map(entry => ({
            SE: entry.SE,
            casos_est: entry.casos_est,
            casos_est_min: entry.casos_est_min,
            casos_est_max: entry.casos_est_max,
            casos: entry.casos,
            p_rt1: entry.p_rt1,
            p_inc100k: entry.p_inc100k,
            nivel: entry.nivel,
            Rt: entry.Rt,
            tempmin: entry.tempmin,
            umidmax: entry.umidmax,
            receptivo: entry.receptivo,
            transmissao: entry.transmissao,
            nivel_inc: entry.nivel_inc,
            umidmed: entry.umidmed,
            umidmin: entry.umidmin,
            tempmed: entry.tempmed,
            tempmax: entry.tempmax,
            notif_accum_year: previousAccumulated + entry.casos,
        }));
    } catch (error) {
        console.error(`Erro ao buscar dados para ${cityName} (${geocode}):`, error.message);
        return null;
    }
}

async function aggregateStateData(citiesData) {
    const stateData = {
        SE: citiesData[0].SE,
        total_week_cases: 0,
        cities_in_alert_state: 0,
        total_notif_accum_year: 0,
        cities: []
    };

    for (const city of citiesData) {
        stateData.total_week_cases += city.casos || 0;
        stateData.total_notif_accum_year += city.notif_accum_year || 0;

        if (city.nivel > 1) stateData.cities_in_alert_state++;

        stateData.cities.push({
            city: city.name,
            geocode: city.geocode,
            casos: city.casos || 0,
            notif_accum_year: city.notif_accum_year,
            nivel_inc: city.nivel_inc,
            p_rt1: city.p_rt1,
            p_inc100k: city.p_inc100k,
            nivel: city.nivel || 1,
            Rt: city.Rt || 0,
            tempmin: city.tempmin,
            umidmax: city.umidmax,
            receptivo: city.receptivo,
            transmissao: city.transmissao,
            umidmed: city.umidmed,
            umidmin: city.umidmin,
            tempmed: city.tempmed,
            tempmax: city.tempmax
        });
    }

    return stateData;
}

async function updateStateDatabase(db, newStateData) {
    const stateCollection = db.collection("statev3");
    await stateCollection.insertOne(newStateData);
    console.log("🟢 Dados do estado atualizados com sucesso!");
}

// Função principal
async function updateState() {
    const startTime = performance.now();
    try {
        await client.connect();
        const db = client.db("denguemg");

        const seData = await getLastEpidemiologicalWeek(db);
        if (!seData) return;

        const cities = await fetchCitiesMG();
        if (cities.length === 0) {
            console.log("Nenhuma cidade encontrada. Encerrando.");
            return;
        }

        let citiesData = [];
        let hasError = false;

        await Promise.all(
            cities.map(async (city) => {
                try {
                    const dengueData = await fetchDengueData(db, city.geocode, city.name, seData.ew_start, seData.ew_end, seData.ey_start, seData.ey_end);
                    if (dengueData && dengueData.length > 0) {
                        citiesData.push({ ...city, ...dengueData[0] });
                    } else {
                        throw new Error(`Dados não encontrados para ${city.name} (${city.geocode})`);
                    }
                } catch (error) {
                    console.error(`Erro ao processar ${city.name}:`, error.message);
                    hasError = true;
                }
            })
        );

        if (hasError) {
            console.log("🔴 Erro ao processar uma ou mais cidades. Nenhum dado será atualizado.");
            return;
        }

        if (citiesData.length > 0) {
            const newStateData = await aggregateStateData(citiesData);
            await updateStateDatabase(db, newStateData);
        }

        console.log(`✅ Atualização concluída! ⏳ Tempo total: ${((performance.now() - startTime) / 1000).toFixed(2)} segundos.`);
    } catch (error) {
        console.error("Erro durante a execução:", error);
    } finally {
        await client.close();
    }
}

// Rota para disparar a atualização
app.get('/update', async (req, res) => {
    try {
        await updateState();
        res.send('Atualização concluída com sucesso!');
    } catch (error) {
        res.status(500).send('Erro ao executar a atualização: ' + error.message);
    }
});

// Rota raiz para evitar erros
app.get('/', (req, res) => {
    res.send('API de atualização de dengue está rodando!');
});

// Iniciar o servidor
app.listen(port, () => {
    console.log(`Servidor rodando na porta ${port}`);
});

