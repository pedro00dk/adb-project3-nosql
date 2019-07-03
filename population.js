function bucketGenerator(elements, properties, mapper) {
    const buckets = {}

    const getBucket = (buckets, values) => {
        let cursor = buckets
        values.forEach((value, i) => {
            if (cursor[value] == undefined) cursor[value] = i === values.length - 1 ? [] : {}
            cursor = cursor[value]
        })
        return cursor
    }

    elements.forEach(element => {
        const values = properties.map(property =>
            typeof property === 'string' ? element[property] : property(element)
        )
        getBucket(buckets, values).push(mapper(element))
    })

    return buckets
}

function gaussianGenerator(mean = 0, std = 1) {
    return ([...new Array(6)].reduce(value => value + Math.random(), 0) - 3) * std + mean
}

function randomDateGenerator(from, to = new Date()) {
    return new Date((+from) + Math.random() * ((+to) - (+from)))
}

function randomSample(samples) {
    return samples[Math.round(Math.random() * (samples.length - 1))]
}

// const rawPeople = JSON.parse(require('fs').readFileSync('./people.json')).results
const rawPeople = JSON.parse(cat('./people.json')).results

const states = [...new Set(rawPeople.map(p => p.location.state))]
const cities = [...new Set(rawPeople.map(p => p.location.city))]
const streets = [...new Set(rawPeople.map(p => p.location.street))]

const driverRatio = 0.05
const rawDrivers = rawPeople.slice(0, Math.floor(rawPeople.length * driverRatio))
rawDrivers.forEach(driver => (driver.type = 'driver'))
const drivers = {}
rawDrivers.forEach(
    rawDriver =>
        (drivers[rawDriver.login.uuid] = {
            uuid: rawDriver.login.uuid,
            name: `${rawDriver.name.first} ${rawDriver.name.last}`,
            email: rawDriver.email,
            pwd: rawDriver.password,
            address: {
                state: rawDriver.location.state,
                city: rawDriver.location.city,
                street: rawDriver.location.street
            },
            phone: rawDriver.phone,
            cnh: {
                number: 123,
                expire: randomDateGenerator(new Date(new Date().getFullYear() + 1, 1)),
                type: randomSample(['ab', 'b', 'c', 'd'])
            }
        })
)
// const driversStateBuckets = bucketGenerator(Object.values(drivers), [d => d.address.state], d => d)
const driversStateBuckets = bucketGenerator(Object.keys(drivers).map(uuid => drivers[uuid]), [d => d.address.state], d => d)

const trips = []
const tripCount = 100000
for (let i = 0; i < tripCount; i++) {
    // console.log(`trip ${i}`)
    print(`generating trip ${i}`)
    const state = randomSample(states)
    const pickupCity = randomSample(cities)
    const destinationCity = randomSample(cities)
    const pickupStreet = randomSample(streets)
    const destinationStreet = randomSample(streets)

    const pickupAddress = { state, city: pickupCity, street: pickupStreet }
    const destinationAddress = { state, city: destinationCity, street: destinationStreet }
    const distance = gaussianGenerator(8, 2)
    const estimatedValue = Math.min(6, 2 + distance * 1.5)
    const finalValue = Math.min(6, estimatedValue * (Math.random() * 0.4 + 0.8))
    const date = randomDateGenerator(new Date(2010, 1, 1))

    const driver = randomSample(driversStateBuckets[state])

    const vehicle = {}
    const passengers = {}
    const payment = {}

    const rating = {}

    trips.push({
        pickupAddress,
        destinationAddress,
        distance,
        estimatedValue,
        finalValue,
        date,
        driver
    })
}

// console.log('done')
print('done')

// MONGO ONLY
db.createCollection('trips')
db.trips.insert(trips)
