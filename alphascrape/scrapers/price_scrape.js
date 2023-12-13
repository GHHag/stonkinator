const puppeteer = require('puppeteer');
// add import for dotenv

async function scrapePrice(url, props) {
    const browser = await puppeteer.launch();
    const page = await browser.newPage();
    await page.goto(url);

    let date = new Date().toISOString().slice(0, 16).replace('T', ' ');
    let data = new Object();
    data['date_time'] = date;

    for (let keys of Object.keys(props)) {
        for (let prop of Object.keys(props[keys])) {
            await page.waitForXPath(keys);
            let [el] = await page.$x(keys);
            let val = await el.getProperty(prop);
            val = await val.jsonValue();
            val = val.replace(/\s+/g, "").replace(',', '.');
            if (val.match(/\.\d+/)) {
                val = parseFloat(val);
            }
            data[props[keys][prop]] = val;
        }
    }

    await fetch(
        `http://${process.env.TET_API_SERVICE}:${process.env.TET_API_PORT}${process.env.API_URL}/price`,
        {
            method: 'POST',
            headers: {
                'Accept': 'application/json',
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(
                {
                    data: data
                }
            )
        }
    )
        .then(response => response.json())
        .catch(err => {
            console.log(err);
        });
}

scrapePrice(
    process.env.PRICE_SCRAPE_URL,
    {
        '//*[@id="knowledge-finance-wholepage__entity-summary"]/div/g-card-section/div/g-card-section/div[1]/div/div/span': { 'textContent': 'symbol' },
        '//*[@id="knowledge-finance-wholepage__entity-summary"]/div/g-card-section/div/g-card-section/div[2]/div[1]/span[1]/span/span': { 'textContent': 'price' }
    }
)