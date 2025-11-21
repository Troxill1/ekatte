import settlements from "./data/settlements.json" with { type: "json" };
import cityHalls from "./data/city-halls.json" with { type: "json" };
import municipalities from "./data/municipalities.json" with { type: "json" };
import regions from "./data/regions.json" with { type: "json" };

const extractSettlements = () => {
    try {
        const data = [];

        settlements.pop();
        settlements.forEach((s, id) => {
            const obj = {
                id: id + 1,
                name: s.name,
                ekatte: s.ekatte,
                municCode: s.obshtina
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

const importData = () => {
    try {
        const settlements = extractSettlements();
        const cityHalls = extractCityHalls();
        const municipalities = extractMunicipalities();
        const regions = extractRegions();

        settlements.forEach(s => {
            const sql = "INSERT INTO settlements(id, name, ekatte, munic_code, type) VALUES(?, ?, ?, ?, ?)";
        });

        cityHalls.forEach(c => {
            const sql = "INSERT INTO city_halls(id, name, ekatte) VALUES(?, ?, ?)";
        });

        municipalities.forEach(m => {
            const sql = "INSERT INTO municipalities(id, name, code, region_code, ekatte) VALUES(?, ?, ?, ?, ?)";
        });

        regions.forEach(r => {
            const sql = "INSERT INTO regions(id, name, code, ekatte) VALUES(?, ?, ?, ?)";
        });

        // TODO: get a library to query the DB
    } catch (err) {
        console.log(err?.message || err);
    }
    finally {
        // TODO: truncate all tables or just the ones that failed
    }
};

importData();
