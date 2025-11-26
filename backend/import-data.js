import settlements from "./data/settlements.json" with { type: "json" };
import cityHalls from "./data/city-halls.json" with { type: "json" };
import municipalities from "./data/municipalities.json" with { type: "json" };
import regions from "./data/regions.json" with { type: "json" };
import { client } from "./db-connection.js";

// TODO: combine extract functions into one

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

const validateJson = (loc, locationType) => {
    if (!loc.name || typeof loc.name !== "string") return false;
    if (!loc.ekatte || typeof loc.name !== "string") return false;

    if (locationType === "region") {
        if (!loc.code || typeof loc.code !== "string") return false;
        
    } else if (locationType === "municipality") {
        if (!loc.code || typeof loc.code !== "string") return false;
        if (!loc.regionCode || typeof loc.regionCode !== "string") return false;

    } else if (locationType === "settlement") {
        if (!loc.municCode || typeof loc.municCode !== "string") return false;
        if (!loc.type || typeof loc.type !== "string") return false;
    }

    return true;
};

const importData = async (query, locations, locationType = "city hall") => {
    const ids = [], names = [], ekattes = [];
    const params = [ids, names, ekattes];
    const loc = locations[0];

    // Add the required parameters for each location type
    if (locationType === "region") {
        const codes = locations.map(l => l.code);
        
        params.push(codes);
    } else if (locationType === "municipality") {
        const codes = locations.map(l => l.code);
        const regionCodes = locations.map(l => l.regionCode);;

        params.push(codes, regionCodes);
    } else if (locationType === "settlement") {
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
    } catch (err) {
        console.log(err?.message || err);
        (await client).query("ROLLBACK");
    }
};

const execute = async () => {
    const regions = extractRegions().filter(r => validateJson(r, "region"));
    let sql = "INSERT INTO regions(id, name, ekatte, code)" +
        "SELECT * FROM UNNEST ($1::bigint[], $2::text[], $3::text[], $4::text[])" +
        "ON CONFLICT (ekatte, code) DO NOTHING";
    await importData(sql, regions, "region");

    const municipalities = extractMunicipalities().filter(m => validateJson(m, "municipality"));
    sql = "INSERT INTO municipalities(id, name, ekatte, code, region_code)" +
        "SELECT * FROM UNNEST ($1::bigint[], $2::text[], $3::text[], $4::text[], $5::text[])" +
        "ON CONFLICT (ekatte, code) DO NOTHING";
    await importData(sql, municipalities, "municipality");

    const settlements = extractSettlements().filter(s => validateJson(s, "settlement"));
    sql = "INSERT INTO settlements(id, name, ekatte, munic_code, type)" +
        "SELECT * FROM UNNEST ($1::bigint[], $2::text[], $3::text[], $4::text[], $5::text[])" +
        "ON CONFLICT (ekatte) DO NOTHING";
    await importData(sql, settlements, "settlement");

    const cityHalls = extractCityHalls().filter(c => validateJson(c));
    sql = "INSERT INTO city_halls(id, name, ekatte)" +
        "SELECT * FROM UNNEST ($1::bigint[], $2::text[], $3::text[])" +
        "ON CONFLICT (ekatte) DO NOTHING";
    await importData(sql, cityHalls);
    
    (await client).release();
};

execute();
