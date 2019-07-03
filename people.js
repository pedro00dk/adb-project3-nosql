const fs = require('fs')
const axios = require('axios').default

const peopleCount = 7500

axios.get(`https://randomuser.me/api/?nat=br&noinfo&results=${peopleCount}`)
    .then(response => {
        fs.writeFileSync('./people.json', JSON.stringify(response.data, undefined, 4))
    })
    .catch(error => console.log(error.message))
