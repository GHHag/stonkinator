require('dotenv').config({ path: `${process.cwd()}/../../.env` });
const puppeteer = require('puppeteer');
const axios = require('axios').default;

const OMX_URL = process.env.OMX_URL;
const FIRST_NORTH_URL = process.env.FIRST_NORTH_URL;
const NORDIC_LARGE_CAPS_URL = process.env.NORDIC_LARGE_CAPS_URL;
const NORDIC_MID_CAPS_URL = process.env.NORDIC_MID_CAPS_URL;
const NORDIC_SMALL_CAPS_URL = process.env.NORDIC_SMALL_CAPS_URL;

const OMXS30_URL = process.env.OMXS30_URL;
const FIRST_NORTH25_URL = process.env.FIRST_NORTH25_URL;

async function scrapeOmxStockSymbols(url, marketListId, reqEndpoint) {
    const browser = await puppeteer.launch({ headless: false });
    const page = await browser.newPage();
    await page.goto(url);
    await page.waitForXPath('//*[@id="listedCompanies"]/tbody');
    const [el] = await page.$x('//*[@id="listedCompanies"]/tbody');
    const txt = await el.getProperty('textContent');
    const rawTxt = await txt.jsonValue();
    let splitRawTxt = rawTxt.split(/\r?\n/).filter((a) => a !== '');

    for (let i = 0; i < splitRawTxt.length; i += 6) {
        try {
            const instrumentDataObj = {
                instrument: splitRawTxt[i],
                symbol: splitRawTxt[i + 1].replace(' ', '_'),
                industry: splitRawTxt[i + 4]
            }
            await axios.post(
                `${reqEndpoint}?id=${marketListId}`,
                instrumentDataObj
            ).then((response) => {
                console.log(response);
            }).catch((err) => {
                console.log(err);
            });
        } catch (err) {
            // logga vilka instrument som ger error till fil
            console.log(splitRawTxt[i]);
            continue;
        }
    }

    await browser.close();
}

async function scrapeNasdaqStockSymbols(url, marketListId, reqEndpoint) {
    const browser = await puppeteer.launch({ headless: false });
    const page = await browser.newPage();
    await page.goto(url);
    const data = await page.evaluate(() => {
        const trs = Array.from(document.querySelectorAll('#sharesInIndexTable tbody tr'));
        return trs.map(tr => tr.getAttribute('title'));
    });

    for (let i = 0; i < data.length; i++) {
        try {
            splitDataText = data[i].split(' - ');
            const instrumentDataObj = {
                instrument: splitDataText[1],
                symbol: splitDataText[0].replace(' ', '_')
            }
            await axios.post(
                `${reqEndpoint}?id=${marketListId}`,
                instrumentDataObj
            ).then((response) => {
                console.log(response);
            }).catch((err) => {
                console.log(err);
            });
        }
        catch (err) {
            // logg vilka instrument som ger error till fil
            console.log(splitDatatext[1]);
            continue;
        }
    }

    await browser.close();
}

const marketListPostReqEndpoint =
    // `http://${process.env.STONKINATOR_API_SERVICE}:${process.env.STONKINATOR_API_PORT}${process.env.API_URL}/market-list`;
    `http://0.0.0.0:${process.env.STONKINATOR_API_PORT_EXP}${process.env.API_URL}/market-list`;

const marketListIdGetReqEndpoint = marketListPostReqEndpoint;

const instrumentsPostReqEndpoint =
    // `http://${process.env.STONKINATOR_API_SERVICE}:${process.env.STONKINATOR_API_PORT}${process.env.API_URL}/instruments`;
    `http://0.0.0.0:${process.env.STONKINATOR_API_PORT_EXP}${process.env.API_URL}/instruments`;

let scrapePipelines = [];

const omxspi_scrape_pipeline = {
    urls: [OMX_URL],
    scraperFunc: scrapeOmxStockSymbols,
    marketList: 'omxspi',
    postReqEndpoint: instrumentsPostReqEndpoint
}
scrapePipelines.push(omxspi_scrape_pipeline);

const omx_scrape_pipeline = {
    urls: [OMX_URL, FIRST_NORTH_URL],
    scraperFunc: scrapeOmxStockSymbols,
    marketList: 'omxs',
    postReqEndpoint: instrumentsPostReqEndpoint
}
scrapePipelines.push(omx_scrape_pipeline);

const omx_large_caps_scrape_pipeline = {
    urls: [NORDIC_LARGE_CAPS_URL],
    scraperFunc: scrapeOmxStockSymbols,
    marketList: 'omxs_large_caps',
    postReqEndpoint: instrumentsPostReqEndpoint
}
scrapePipelines.push(omx_large_caps_scrape_pipeline);

const omx_mid_caps_scrape_pipeline = {
    urls: [NORDIC_MID_CAPS_URL],
    scraperFunc: scrapeOmxStockSymbols,
    marketList: 'omxs_mid_caps',
    postReqEndpoint: instrumentsPostReqEndpoint
}
scrapePipelines.push(omx_mid_caps_scrape_pipeline);

const omx_small_caps_scrape_pipeline = {
    urls: [NORDIC_SMALL_CAPS_URL],
    scraperFunc: scrapeOmxStockSymbols,
    marketList: 'omxs_small_caps',
    postReqEndpoint: instrumentsPostReqEndpoint
}
scrapePipelines.push(omx_small_caps_scrape_pipeline);

const first_north_scrape_pipeline = {
    urls: [FIRST_NORTH_URL],
    scraperFunc: scrapeOmxStockSymbols,
    marketList: 'first_north',
    postReqEndpoint: instrumentsPostReqEndpoint
}
scrapePipelines.push(first_north_scrape_pipeline);

const omxs30_scrape_pipeline = {
    urls: [OMXS30_URL],
    scraperFunc: scrapeNasdaqStockSymbols,
    marketList: 'omxs30',
    postReqEndpoint: instrumentsPostReqEndpoint
}
scrapePipelines.push(omxs30_scrape_pipeline);

const first_north25_scrape_pipeline = {
    urls: [FIRST_NORTH25_URL],
    scraperFunc: scrapeNasdaqStockSymbols,
    marketList: 'first_north25',
    postReqEndpoint: instrumentsPostReqEndpoint
}
scrapePipelines.push(first_north25_scrape_pipeline);

const runScrapePipeline = async () => {
    for (let { urls, scraperFunc, marketList, postReqEndpoint } of scrapePipelines) {
        await axios.post(
            marketListPostReqEndpoint,
            { market_list: marketList }
        ).then((response) => {
                console.log(response);
        }).catch((err) => {
            console.log(err);
        });

        // någon funktionalitet för att återstaella mongo db collection innan ny data insertas
        //cleanUpCollection();

        let marketListId;
        await axios.get(
            `${marketListIdGetReqEndpoint}?market-list=${marketList}`
        ).then((response) => {
            console.log(response.data);
            marketListId = response.data._id;
        }).catch((err) => {
            console.log(err);
        });

        for (let url of urls) {
            scraperFunc(url, marketListId, postReqEndpoint);
        }
    }
}

runScrapePipeline();