import settlements from "./data/settlements.json" with { type: "json" };
import cityHalls from "./data/city-halls.json" with { type: "json" };
import municipalities from "./data/municipalities.json" with { type: "json" };
import regions from "./data/regions.json" with { type: "json" };
import { client } from "./db-connection.js";

const extractSettlements = () => {
    try {
        const data = [];

        settlements.pop();
        settlements.forEach((s, id) => {
            const obj = {
                id: id + 1,
                name: s.name,
                ekatte: s.ekatte,
                municCode: s.obshtina,
                type: s.t_v_m
            };

            data.push(obj);
        });

        return data;
    } catch (err) {
        console.log(err?.message || err);
    }
};

const extractCityHalls = () => {
    try {
        const data = [];

        cityHalls.pop();
        cityHalls.forEach((c, id) => {
            const obj = {
                id: id + 1,
                name: c.name,
                ekatte: c.ekatte
            };

            data.push(obj);
        });

        return data;
    } catch (err) {
        console.log(err?.message || err);
    }
};

const extractMunicipalities = () => {
    try {
        const data = [];

        municipalities.pop();
        municipalities.forEach((m, id) => {
            const code = m.obshtina;
            const regionCode = code.substring(0, 3);

            const obj = {
                id: id + 1,
                name: m.name,
                code,
                regionCode,
                ekatte: m.ekatte
            };

            data.push(obj);
        });

        return data;
    } catch (err) {
        console.log(err?.message || err);
    }
};

const extractRegions = () => {
    try {
        const data = [];

        regions.pop();
        regions.forEach((r, id) => {
            const obj = {
                id: id + 1,
                name: r.name,
                code: r.oblast,
                ekatte: r.ekatte
            };

            data.push(obj);
        });

        return data;
    } catch (err) {
        console.log(err?.message || err);
    }
};

const importData = async () => {
    try {
        const settlements = extractSettlements();
        const cityHalls = extractCityHalls();
        const municipalities = extractMunicipalities();
        const regions = extractRegions();

        // TODO: validate extracted data
        // TODO: replace the current, slow insert with bulk insert

        for (const r of regions) {
            const sql = "INSERT INTO regions(id, name, code, ekatte) VALUES($1, $2, $3, $4) ON CONFLICT (ekatte, code) DO NOTHING";
            const params = [r.id, r.name, r.code, r.ekatte];
            (await client).query(sql, params);
        }

        for (const m of municipalities) {
            const sql = "INSERT INTO municipalities(id, name, code, region_code, ekatte) VALUES($1, $2, $3, $4, $5) ON CONFLICT (ekatte, code) DO NOTHING";
            const params = [m.id, m.name, m.code, m.regionCode, m.ekatte];
            (await client).query(sql, params);
        }

        for (const s of settlements) {
            const sql = "INSERT INTO settlements(id, name, ekatte, munic_code, type) VALUES($1, $2, $3, $4, $5) ON CONFLICT (ekatte) DO NOTHING";
            const params = [s.id, s.name, s.ekatte, s.municCode, s.type];
            (await client).query(sql, params);
        }

        for (const c of cityHalls) {
            const sql = "INSERT INTO city_halls(id, name, ekatte) VALUES($1, $2, $3) ON CONFLICT (ekatte) DO NOTHING";
            const params = [c.id, c.name, c.ekatte];
            (await client).query(sql, params);
        }
    } catch (err) {
        console.log(err?.message || err);
    }
    finally {
        (await client).release();
    }
};

importData();
