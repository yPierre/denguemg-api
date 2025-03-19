const express = require('express');
const { MongoClient } = require("mongodb");
const { performance } = require("perf_hooks");
const https = require('https');

const app = express();
const port = process.env.PORT || 3000;

const uri = process.env.MONGODB_URI || "mongodb+srv://jeandias1997:GWtDa5xFwnKaYhgG@cluster0.njfbl.mongodb.net/";
const client = new MongoClient(uri);

const agent = new https.Agent({
    secureProtocol: 'TLSv1_2_method'
});

async function fetchWithRetry(url, retries = 3) {
    for (let i = 0; i < retries; i++) {
        try {
            const response = await fetch(url, {
                method: "GET",
                headers: { "Content-Type": "application/json" },
                agent: agent
            });
            if (response.ok) return response;
        } catch (error) {
            if (i === retries - 1) throw error;
        }
    }
}

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

async function getEpidemiologicalWeeks(db, numWeeks = 2) {
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
        ew_end = week + 1; // Próxima SE
        ew_start = week - 1;   // SE anterior
        ey_start = year;
        ey_end = year;
    } else {
        ew_end = 1;
        ew_start = 52;
        ey_start = year;
        ey_end = year + 1;
    }

    return { ew_start, ew_end, ey_start, ey_end };
}

async function fetchCitiesMG() {
    const urlIBGE = "https://servicodados.ibge.gov.br/api/v1/localidades/estados/31/municipios";
    try {
        const response = await fetchWithRetry(urlIBGE);
        const cities = await response.json();
        return cities.map(city => ({ geocode: city.id, name: city.nome }));
    } catch (error) {
        //console.error("Erro ao obter cidades de MG:", error.message);
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
        const response = await fetchWithRetry(`${apiUrl}?${params}`);
        if (!response.ok) {
            //console.warn(`⚠️ API InfoDengue retornou erro para ${cityName} (${geocode}): ${response.status}`);
            return null;
        }

        const data = await response.json();
        if (!data || data.length === 0) {
            //console.warn(`⚠️ Nenhum dado retornado para ${cityName} (${geocode})`);
            return null;
        }

        const result = {};
        for (const entry of data) {
            const se = entry.SE.toString();
            const previousAccumulated = await getPreviousCityAccumulated(db, geocode, se);
            result[se] = {
                SE: se,
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
                versao_modelo: entry.versao_modelo || "N/A" // Adiciona mesmo que não exista ainda
            };
        }

        return result;
    } catch (error) {
        //console.error(`Erro ao buscar dados para ${cityName} (${geocode}):`, error.message);
        return null;
    }
}

async function aggregateStateData(citiesData, se) {
    const stateData = {
        SE: se,
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
            tempmax: city.tempmax,
            versao_modelo: city.versao_modelo
        });
    }

    return stateData;
}

async function updateStateDatabase(db, citiesDataBySE) {
    const stateCollection = db.collection("statev3");

    for (const se of Object.keys(citiesDataBySE)) {
        const existingData = await stateCollection.findOne({ SE: se });
        const newData = await aggregateStateData(citiesDataBySE[se], se);

        if (!existingData) {
            // Inserir nova SE
            await stateCollection.insertOne(newData);
            //log(`Nova SE inserida: ${se}`);
        } else {
            // Atualizar apenas os campos especificados
            await stateCollection.updateOne(
                { SE: se },
                {
                    $set: {
                        total_week_cases: newData.total_week_cases,
                        cities_in_alert_state: newData.cities_in_alert_state,
                        total_notif_accum_year: newData.total_notif_accum_year,
                        "cities": newData.cities.map(city => ({
                            city: city.city,
                            geocode: city.geocode,
                            casos: city.casos,
                            notif_accum_year: city.notif_accum_year,
                            nivel_inc: city.nivel_inc,
                            p_rt1: city.p_rt1,
                            p_inc100k: city.p_inc100k,
                            nivel: city.nivel,
                            Rt: city.Rt,
                            tempmin: city.tempmin,
                            umidmax: city.umidmax,
                            receptivo: city.receptivo,
                            transmissao: city.transmissao,
                            umidmed: city.umidmed,
                            umidmin: city.umidmin,
                            tempmed: city.tempmed,
                            tempmax: city.tempmax,
                            versao_modelo: city.versao_modelo
                        }))
                    }
                }
            );
            //console.log(`SE atualizada: ${se}`);
        }
    }
}

async function updateState() {
    console.time("Tempo total de execução");
    try {
        await client.connect();
        const db = client.db("denguemg");

        const seData = await getEpidemiologicalWeeks(db, 2);
        if (!seData) return;

        const cities = await fetchCitiesMG();
        if (cities.length === 0) {
            //console.log("Nenhuma cidade encontrada. Encerrando.");
            return;
        }

        let citiesDataBySE = {};
        const seList = [
            `${seData.ey_start}${seData.ew_start.toString().padStart(2, '0')}`,
            `${seData.ey_end}${seData.ew_end.toString().padStart(2, '0')}`
        ];
        for (const se of seList) {
            citiesDataBySE[se] = [];
        }

        const batchSize = 50;
        for (let i = 0; i < cities.length; i += batchSize) {
            const batch = cities.slice(i, i + batchSize);
            await Promise.all(
                batch.map(async (city) => {
                    try {
                        const dengueData = await fetchDengueData(db, city.geocode, city.name, seData.ew_start, seData.ew_end, seData.ey_start, seData.ey_end);
                        if (dengueData) {
                            for (const se of seList) {
                                if (dengueData[se]) {
                                    citiesDataBySE[se].push({ ...city, ...dengueData[se] });
                                }
                            }
                        }
                    } catch (error) {
                        //console.error(`Erro ao processar ${city.name}:`, error.message);
                    }
                })
            );
        }

        await updateStateDatabase(db, citiesDataBySE);
        console.timeEnd("Tempo total de execução");
    } catch (error) {
        console.error("Erro durante a execução:", error);
    } finally {
        await client.close();
    }
}

app.get('/update', async (req, res) => {
    try {
        await updateState();
        res.send('Atualização concluída com sucesso!');
    } catch (error) {
        res.status(500).send('Erro ao executar a atualização: ' + error.message);
    }
});

app.get('/', (req, res) => {
    res.send('API de atualização de dengue está rodando!');
});

app.listen(port, () => {
    console.log(`Servidor rodando na porta ${port}`);
});