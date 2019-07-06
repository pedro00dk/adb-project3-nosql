// use carservice
db = db.getSiblingDB('carservice')

// proporção de viagens cujo rating foi 5
print(db.trips.count({ rating: 5 }) / db.trips.count())

// listar ids dos passageiros das viagens com 12 ou mais quilometros de distância
db.trips.find({ distance: { $gte: 12 } }, { 'passenger': 1, distance: 1 }).pretty()

// listar os estados por maior quantidade de viagens feitas
db.trips.aggregate([{ $group: { _id: '$pickupAddress.state', count: { $sum: 1 } } }, { $sort: { count: -1 } }])

// listar motoristas e seus estados, ordenados pelo faturamento do motorista
db.trips.aggregate([
    { $lookup: { from: 'people', localField: 'driver', foreignField: '_id', as: 'driver' } },
    { $unwind: '$driver' },
    {
        $group: {
            _id: { cpf: '$driver.cpf', name: '$driver.name', state: '$driver.address.state' },
            total: { $sum: '$finalValue' }
        }
    },
    { $sort: { total: -1 } }
])

// listar motoristas do estado de pernambuco apenas, ordenados pelo faturamento do motorista
db.trips.aggregate([
    { $lookup: { from: 'people', localField: 'driver', foreignField: '_id', as: 'driver' } },
    { $unwind: '$driver' },
    { $match: { 'driver.address.state': 'pernambuco' } },
    {
        $group: {
            _id: { name: '$driver.name', state: '$driver.address.state', cpf: '$driver.cpf' },
            total: { $sum: '$finalValue' }
        }
    },
    { $sort: { total: -1 } }
])

// cpf de motoristas que possuem carro a partir de 2018
db.trips.aggregate([
    { $match: { 'vehicle.year': { $gte: 2018 } } },
    { $lookup: { from: 'people', localField: 'driver', foreignField: '_id', as: 'driver' } },
    { $unwind: '$driver' },
    { $group: { _id: { cpf: '$driver.cpf', carYear: '$vehicle.year' } } }
])

// quantidade de viagens que os passageiros fizeram entre 2015 e 2017
db.trips.aggregate([
    { $lookup: { from: 'people', localField: 'passenger', foreignField: '_id', as: 'passenger' } },
    { $unwind: '$passenger' },
    { $match: { $and: [{ date: { $gte: new Date(2015, 1) } }, { date: { $lt: new Date(2019, 1, 1) } }] } },
    { $group: { _id: { cpf: '$passenger.name', name: '$passenger.name' }, count: { $sum: 1 } } },
    { $sort: { count: -1 } }
])

// distancia média percorrida por estado
db.trips.aggregate([
    { $group: { _id: '$pickupAddress.state', avgDistance: { $avg: '$distance' } } },
    { $sort: { avgDistance: -1 } }
])

// distancia max percorrida por estado
db.trips.aggregate([
    { $group: { _id: '$pickupAddress.state', maxDistance: { $max: '$distance' } } },
    { $sort: { maxDistance: -1 } }
])

// primeiros 20 motoristas que possuem carros a partir de 2015
db.trips.aggregate([
    { $lookup: { from: 'people', localField: 'driver', foreignField: '_id', as: 'driver' } },
    { $unwind: '$driver' },
    { $match: { 'vehicle.year': { $gte: 2015 } } },
    { $group: { _id: { cpf: '$driver.cpf', name: '$driver.name', carYear: '$vehicle.year' } } },
    { $limit: 20 }
])

// contar viagens cuja a cidade do passageiro é igual a do motorista.
db.trips.aggregate([
    { $lookup: { from: 'people', localField: 'driver', foreignField: '_id', as: 'driver' } },
    { $lookup: { from: 'people', localField: 'passenger', foreignField: '_id', as: 'passenger' } },
    { $unwind: '$driver' },
    { $unwind: '$passenger' },
    { $match: { $expr: { $eq: ['$driver.address.city', '$passenger.address.city'] } } },
    { $count: 'count' }
])

// contar a quantidade de viagens por estado cujo o valor final é maior que o valor estimado
db.trips.aggregate([
    {
        $group: {
            _id: '$pickupAddress.state',
            count: {
                $sum: {
                    $cond: {
                        if: { $gt: ['$estimatedValue', '$finalValue'] },
                        then: 1,
                        else: 0
                    }
                }
            }
        }
    },
    { $sort: { count: -1 } }
])

// ids de passageiros por estado com a maior distancia percorrida
db.trips.mapReduce(
    function() {
        emit(this.pickupAddress.state, { pid: this.passenger, dist: this.distance })
    },
    function(state, values) {
        return values.reduce(
            (acc, next) => ({ name: next.dist > acc.dist ? next.pid : acc.pid, dist: Math.max(acc.dist, next.dist) }),
            { pid: null, dist: 0 }
        )
    },
    { out: 'res' }
)

// Listar a nota média dos motoristas do maranhão
db.trips.aggregate([
    { $lookup: { from: 'people', localField: 'driver', foreignField: '_id', as: 'driver' } },
    { $unwind: '$driver' },
    { $match: { 'driver.address.state': 'maranhão' } },
    { $group: { _id: { cnh: '$driver.cnh.number', nome: '$driver.name' }, avgRating: { $avg: '$rating' } } }
])

// Os 5 passageiros que gastaram mais em corridas nos estados do sul entre 01/09/14 e 03/04/18 ordenador por custo
db.trips.aggregate([
    {
        $match: {
            $and: [
                {
                    $or: [
                        { 'pickupAddress.state': 'santa catarina' },
                        { 'pickupAddress.state': 'parana' },
                        { 'pickupAddress.state': 'rio grande do sul' }
                    ]
                },
                { $and: [{ date: { $gte: new Date(2014, 9, 1) } }, { date: { $lte: new Date(2018, 4, 3) } }] }
            ]
        }
    },
    { $lookup: { from: 'people', localField: 'passenger', foreignField: '_id', as: 'passenger' } },
    { $unwind: '$passenger' },
    { $group: { _id: { id: '$passenger.uuid', name: '$passenger.name' }, sum: { $sum: '$finalValue' } } },
    { $sort: { sum: -1 } },
    { $limit: 5 }
])
