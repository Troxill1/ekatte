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
            const l = {
                id: id + 1,
                name: s.name,
                ekatte: s.ekatte,
                municCode: s.obshtina,
                type: s.t_v_m
            };

            data.push(l);
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
            const l = {
                id: id + 1,
                name: c.name,
                ekatte: c.ekatte
            };

            data.push(l);
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

            const l = {
                id: id + 1,
                name: m.name,
                code,
                regionCode,
                ekatte: m.ekatte
            };

            data.push(l);
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
            const l = {
                id: id + 1,
                name: r.name,
                code: r.oblast,
                ekatte: r.ekatte
            };

            data.push(l);
        });

        return data;
    } catch (err) {
        console.log(err?.message || err);
    }
};

const importData = async (query, locations) => {
    const ids = [], names = [], ekattes = [];
    const params = [ids, names, ekattes];
    const loc = locations[0];

    // Add the required parameters for each location type
    if (loc?.code && !loc?.regionCode) {  // region
        const codes = locations.map(l => l.code);
        
        params.push(codes);
    } else if (loc?.code && loc?.regionCode) {  // municipality
        const codes = locations.map(l => l.code);
        const regionCodes = locations.map(l => l.regionCode);;

        params.push(codes, regionCodes);
    } else if (loc?.municCode && loc?.type) {  // settlement
        const municCodes = locations.map(l => l.municCode);
        const types = locations.map(l => l.type);

        params.push(municCodes, types);
    }
    
    for (const l of locations) {
        ids.push(l.id);
        names.push(l.name);
        ekattes.push(l.ekatte);
    }

    try {
        (await client).query("BEGIN");
        (await client).query(query, params);
        (await client).query("COMMIT");
    } catch (e) {
        console.log(err?.message || err);
        (await client).query("ROLLBACK");
    }
};

const execute = async () => {
    // TODO: validate extracted data

    const regions = extractRegions();
    let sql = "INSERT INTO regions(id, name, ekatte, code)" +
        "SELECT * FROM UNNEST ($1::bigint[], $2::text[], $3::text[], $4::text[])" +
        "ON CONFLICT (ekatte, code) DO NOTHING";
    await importData(sql, regions);

    const municipalities = extractMunicipalities();
    sql = "INSERT INTO municipalities(id, name, ekatte, code, region_code)" +
        "SELECT * FROM UNNEST ($1::bigint[], $2::text[], $3::text[], $4::text[], $5::text[])" +
        "ON CONFLICT (ekatte, code) DO NOTHING";
    await importData(sql, municipalities);

    const settlements = extractSettlements();
    sql = "INSERT INTO settlements(id, name, ekatte, munic_code, type)" +
        "SELECT * FROM UNNEST ($1::bigint[], $2::text[], $3::text[], $4::text[], $5::text[])" +
        "ON CONFLICT (ekatte) DO NOTHING";
    await importData(sql, settlements);

    const cityHalls = extractCityHalls();
    sql = "INSERT INTO city_halls(id, name, ekatte)" +
        "SELECT * FROM UNNEST ($1::bigint[], $2::text[], $3::text[])" +
        "ON CONFLICT (ekatte) DO NOTHING";
    await importData(sql, cityHalls);
    
    (await client).release();
};

execute();
