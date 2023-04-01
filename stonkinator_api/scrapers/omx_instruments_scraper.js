require('dotenv').config('.env');
const puppeteer = require('puppeteer');
const axios = require('axios').default;

const OMX_URL = 'http://www.nasdaqomxnordic.com/aktier/listed-companies/stockholm';
const FIRST_NORTH_URL = 'http://www.nasdaqomxnordic.com/aktier/listed-companies/first-north';
const NORDIC_LARGE_CAPS_URL = 'http://www.nasdaqomxnordic.com/aktier/listed-companies/nordic-large-cap';
const NORDIC_MID_CAPS_URL = 'http://www.nasdaqomxnordic.com/aktier/listed-companies/nordic-mid-cap';
const NORDIC_SMALL_CAPS_URL = 'http://www.nasdaqomxnordic.com/aktier/listed-companies/nordic-small-cap';

const OMXS30_URL = 'http://www.nasdaqomxnordic.com/index/index_info?Instrument=SE0000337842';
const FIRST_NORTH25_URL = 'http://www.nasdaqomxnordic.com/index/index_info?Instrument=SE0007576558';

async function scrapeOmxStockSymbols(url, marketListId, reqEndpoint) {
    const browser = await puppeteer.launch({ headless: false });
    const page = await browser.newPage();
    await page.goto(url);
    await page.waitForXPath('//*[@id="listedCompanies"]/tbody');
    const [el] = await page.$x('//*[@id="listedCompanies"]/tbody');
    const txt = await el.getProperty('textContent');
    const rawTxt = await txt.jsonValue();
    let splitRawTxt = rawTxt.split(/\r?\n/).filter((a) => a !== '');

    let instrumentDataObjects = [];
    for (let i = 0; i < splitRawTxt.length; i += 6) {
        try {
            const instrumentDataObj = {
                instrument: splitRawTxt[i],
                symbol: splitRawTxt[i + 1].replace(' ', '_'),
                industry: splitRawTxt[i + 4]
            }
            instrumentDataObjects.push(instrumentDataObj);
        }
        catch (err) {
            // logga vilka instrument som ger error till fil
            console.log(splitRawTxt[i]);
            continue;
        }
    }

    await axios.post(
        reqEndpoint,
        {
            marketListId: marketListId,
            instrumentDataObjects: instrumentDataObjects,
        }
    )
        .then((response) => {
            console.log(response);
        })
        .catch((err) => {
            console.log(err);
        });

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

    let instrumentDataObjects = [];
    for (let i = 0; i < data.length; i++) {
        try {
            splitDataText = data[i].split(' - ');
            const instrumentDataObj = {
                instrument: splitDataText[1],
                symbol: splitDataText[0].replace(' ', '_')
            }
            instrumentDataObjects.push(instrumentDataObj);
        }
        catch (err) {
            // logga vilka instrument som ger error till fil
            console.log(splitDatatext[1]);
            continue;
        }
    }

    await axios.post(
        reqEndpoint,
        {
            marketListId,
            instrumentDataObjects: instrumentDataObjects,
        }
    )
        .then((response) => {
            console.log(response);
        })
        .catch((err) => {
            console.log(err);
        });

    await browser.close();
}

const marketListPostReqEndpoint =
    `http://${process.env.TET_API_HOST}:${process.env.TET_API_PORT}${process.env.API_URL}/market-list`;

const marketListIdGetReqEndpoint = marketListPostReqEndpoint;

const instrumentsPostReqEndpoint =
    `http://${process.env.TET_API_HOST}:${process.env.TET_API_PORT}${process.env.API_URL}/instruments`;

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
            { marketList: marketList }
        )
            .then((response) => {
                console.log(response);
            })
            .catch((err) => {
                console.log(err);
            });

        // någon funktionalitet för att återstaella mongo db collection innan ny data insertas
        //cleanUpCollection();

        let marketListId;
        await axios.get(
            marketListIdGetReqEndpoint,
            { data: { marketList: marketList } }
        )
            .then((response) => {
                console.log(response.data);
                marketListId = response.data.result._id;
            })
            .catch((err) => {
                console.log(err);
            });

        for (let url of urls) {
            scraperFunc(url, marketListId, postReqEndpoint);
        }
    }
}

runScrapePipeline();
