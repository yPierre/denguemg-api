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
    const stateCollection = db.collection("statev4");
    const previousSE = currentSE - 1;

    // Busca o documento da SE anterior para a cidade
    const previousData = await stateCollection.findOne(
        { SE: previousSE, "cities.geocode": geocode },
        { projection: { "cities.$": 1 } }
    );

    if (!previousData || !previousData.cities || previousData.cities.length === 0) {
        // Se não houver dado anterior, verifica se é um novo ano
        const currentYear = parseInt(currentSE.toString().substring(0, 4));
        const previousYear = previousSE ? parseInt(previousSE.toString().substring(0, 4)) : currentYear - 1;
        return currentYear > previousYear ? 0 : 0; // Reinicia se for novo ano
    }

    return previousData.cities[0].notif_accum_year || 0;
}

async function getEpidemiologicalWeeks(db, numWeeksToUpdate = 1) {
    const stateCollection = db.collection("statev4");
    const stateData = await stateCollection.findOne({}, { sort: { SE: -1 } });

    if (!stateData) {
        throw new Error("Nenhuma informação epidemiológica encontrada no banco.");
    }

    const latestSE = stateData.SE.toString();
    const year = parseInt(latestSE.substring(0, 4));
    const week = parseInt(latestSE.substring(4, 6));

    let ew_start, ew_end, ey_start, ey_end;
    if (week > numWeeksToUpdate) {
        ew_start = week - numWeeksToUpdate;  
        ew_end = week + 1;
        ey_start = year;
        ey_end = year;
    } else {
        const weeksInPrevYear = numWeeksToUpdate - (week - 1);
        ew_start = 53 - weeksInPrevYear; // Volta para o ano anterior
        ew_end = week + 1;
        ey_start = year - 1;
        ey_end = year;
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
            const se = Number(entry.SE);
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
                versao_modelo: entry.versao_modelo,
                tweet: entry.tweet,
                Rt: entry.Rt,
                pop: entry.pop,
                tempmin: entry.tempmin,
                umidmax: entry.umidmax,
                receptivo: entry.receptivo,
                transmissao: entry.transmissao,
                nivel_inc: entry.nivel_inc,
                umidmed: entry.umidmed,
                umidmin: entry.umidmin,
                tempmed: entry.tempmed,
                tempmax: entry.tempmax,
                casprov_est: entry.casprov_est,
                casprov_est_min: entry.casprov_est_min,
                casprov_est_max: entry.casprov_est_max,
                casconf: entry.casconf,
                notif_accum_year: previousAccumulated + entry.casos,
            };
        }

        return result;
    } catch (error) {
        //console.error(`Erro ao buscar dados para ${cityName} (${geocode}):`, error.message);
        return null;
    }
}

async function aggregateStateData(db, citiesData, se) {
    const stateData = {
        SE: se,
        total_week_cases: 0,
        cities_in_alert_state: 0,
        total_notif_accum_year: 0,
        cities: []
    };

    // Verifica se citiesData é iterável
    if (!Array.isArray(citiesData) || !citiesData) {
        console.warn(`Nenhum dado de cidades disponível para SE ${se}. Retornando stateData vazio.`);
        const previousSE = se - 1;
        const previousData = await db.collection("statev4").findOne(
            { SE: previousSE },
            { projection: { total_notif_accum_year: 1 } }
        );
        const previousAccumulated = previousData ? previousData.total_notif_accum_year || 0 : 0;
        const currentYear = parseInt(se.toString().substring(0, 4));
        const previousYear = previousData ? parseInt(previousData.SE.toString().substring(0, 4)) : currentYear - 1;
        stateData.total_notif_accum_year = currentYear > previousYear ? 0 : previousAccumulated;
        return stateData;
    }

    // Calcula total_week_cases e cities_in_alert_state
    for (const city of citiesData) {
        stateData.total_week_cases += city.casos || 0;
        if (city.nivel === 4) stateData.cities_in_alert_state++;
        stateData.cities.push({
            city: city.name,
            geocode: city.geocode,
            casos_est: city.casos_est || 0,
            casos_est_min: city.casos_est_min || 0,
            casos_est_max: city.casos_est_max || 0,
            casos: city.casos || 0,
            p_rt1: city.p_rt1 || 0,
            p_inc100k: city.p_inc100k || 0,
            nivel: city.nivel || 1,
            versao_modelo: city.versao_modelo || "N/A",
            tweet: city.tweet || "N/A",
            Rt: city.Rt || 0,
            pop: city.pop || 0,
            tempmin: city.tempmin || 0,
            umidmax: city.umidmax || 0,
            receptivo: city.receptivo || 0,
            transmissao: city.transmissao || 0,
            nivel_inc: city.nivel_inc || 0,
            umidmed: city.umidmed || 0,
            umidmin: city.umidmin || 0,
            tempmed: city.tempmed || 0,
            tempmax: city.tempmax || 0,
            casprov_est: city.casprov_est || 0,
            casprov_est_min: city.casprov_est_min || 0,
            casprov_est_max: city.casprov_est_max || 0,
            casconf: city.casconf || 0,
            notif_accum_year: city.notif_accum_year || 0
        });
    }

    // Calcula total_notif_accum_year corretamente
    const previousSE = se - 1;
    const previousData = await db.collection("statev4").findOne(
        { SE: previousSE },
        { projection: { total_notif_accum_year: 1, SE: 1 } }
    );
    const previousAccumulated = previousData ? previousData.total_notif_accum_year || 0 : 0;
    const currentYear = parseInt(se.toString().substring(0, 4));
    const previousYear = previousData ? parseInt(previousData.SE.toString().substring(0, 4)) : currentYear - 1;
    stateData.total_notif_accum_year = currentYear > previousYear ? stateData.total_week_cases : previousAccumulated + stateData.total_week_cases;

    return stateData;
}

async function updateStateDatabase(db, citiesDataBySE) {
    const stateCollection = db.collection("statev4");
    const seList = Object.keys(citiesDataBySE).map(Number);
    const latestSE = Math.max(...seList); // Semana recém-adicionada
    const latestData = citiesDataBySE[latestSE];
    const latestModelVersion = latestData.length > 0 ? latestData[0].versao_modelo : null;

    if (!latestModelVersion) {
        console.warn(`Nenhuma versão de modelo encontrada para SE ${latestSE}. Pulando atualização.`);
        return;
    }

    for (const seStr of Object.keys(citiesDataBySE)) {
        const se = Number(seStr);
        const newData = await aggregateStateData(db, citiesDataBySE[seStr], se);

        if (se === latestSE) {
            // Insere a nova semana sem checar versão
            await stateCollection.updateOne(
                { SE: se },
                { $set: newData },
                { upsert: true }
            );
            console.log(`SE ${se} inserida como nova semana.`);
        } else {
            // Atualiza apenas se versao_modelo coincide
            const apiModelVersion = newData.cities.length > 0 ? newData.cities[0].versao_modelo : null;
            if (apiModelVersion === latestModelVersion) {
                await stateCollection.updateOne(
                    { SE: se },
                    { $set: newData },
                    { upsert: false } // Não insere, só atualiza se já existe
                );
            }
        }
    }
}

async function updateState() {
    console.time("Tempo total de execução");
    try {
        await client.connect();
        const db = client.db("denguemg");

        const numWeeksToUpdate = 52;
        const seData = await getEpidemiologicalWeeks(db, numWeeksToUpdate);
        if (!seData) return;

        console.log(`Intervalo da API: ${seData.ey_start}${seData.ew_start.toString().padStart(2, '0')} a ${seData.ey_end}${seData.ew_end.toString().padStart(2, '0')}`);

        const cities = await fetchCitiesMG();
        if (cities.length === 0) {
            console.log("Nenhuma cidade encontrada. Encerrando.");
            return;
        }

        let citiesDataBySE = {};
        const seList = [];

        // Calcula as semanas a partir da última no banco
        const latestSE = await db.collection("statev4").findOne({}, { sort: { SE: -1 } });
        const latestYear = parseInt(latestSE.SE.toString().substring(0, 4));
        const latestWeek = parseInt(latestSE.SE.toString().substring(4, 6));

        // Gera as 5 semanas anteriores + 1 nova
        let currentWeek = latestWeek - numWeeksToUpdate;
        let currentYear = latestYear;
        for (let i = 0; i <= numWeeksToUpdate + 1; i++) { // +1 para incluir a nova semana
            if (currentWeek < 1) {
                currentWeek += 52;
                currentYear -= 1;
            }
            const se = Number(`${currentYear}${currentWeek.toString().padStart(2, '0')}`);
            seList.push(se);
            citiesDataBySE[se] = [];
            currentWeek++;
            if (currentWeek > 52) {
                currentWeek -= 52;
                currentYear += 1;
            }
        }

        console.log("SEs a processar:", seList);

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
                        console.error(`Erro ao processar ${city.name}:`, error.message);
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