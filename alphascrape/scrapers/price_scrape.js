const puppeteer = require('puppeteer');

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
        `http://localhost:6969/api/insert`,
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
    'https://www.google.se/search?q=omxs30&source=hp&ei=nBORY407xYXFzw_qnIqIAQ&iflsig=AJiK0e8AAAAAY5EhrBDpxePm1Io8XlCN62XksxxBFG_x&ved=0ahUKEwiN-dyLx-j7AhXFQvEDHWqOAhEQ4dUDCAc&uact=5&oq=omxs30&gs_lcp=Cgdnd3Mtd2l6EAMyEAgAEIAEELEDEIMBEEYQ-gEyBQgAEIAEMgUIABCABDIFCAAQgAQyBQgAEIAEMgUIABCABDIFCAAQgAQyBQgAEIAEMgUIABCABDIFCAAQgAQ6CwgAEIAEELEDEIMBOggILhCxAxCDAToOCC4QsQMQgwEQxwEQ0QM6BQguEIAEOggIABCABBCxAzoOCC4QgAQQsQMQxwEQ0QM6CAguEIAEELEDOg4ILhCABBDHARDRAxDUAjoOCC4QgAQQsQMQgwEQ1AI6CggAEIAEELEDEAo6BwguEIAEEAo6BwgAEIAEEAo6DQguEIAEEMcBEK8BEApQAFjJHGCjHWgCcAB4AIAByQGIAb8FkgEFNi4xLjGYAQCgAQE&sclient=gws-wiz',
    {
        '//*[@id="knowledge-finance-wholepage__entity-summary"]/div/g-card-section/div/g-card-section/div[1]/div/div/span': { 'textContent': 'symbol' },
        '//*[@id="knowledge-finance-wholepage__entity-summary"]/div/g-card-section/div/g-card-section/div[2]/div[1]/span[1]/span/span': { 'textContent': 'price' }
    }
)