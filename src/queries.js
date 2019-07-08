// use carservice
db = db.getSiblingDB('carservice')

// proporção de viagens cujo rating foi 5
print(db.trips.count({ rating: 5 }) / db.trips.count())

// listar ids dos passageiros das viagens com 12 ou mais quilometros de distância
db.trips.find({ distance: { $gte: 12 } }, { passenger: 1, distance: 1 }).pretty()
// same query with $where
db.trips
    .find(
        {
            $where: function() {
                return this.distance >= 12
            }
        },
        { passenger: 1, distance: 1 }
    )
    .pretty()

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
// renomeando nome ruim da coleção
db.res.renameCollection('highestPassengerDistance')

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
    { $group: { _id: { cpf: '$passenger.cpf', name: '$passenger.name' }, sum: { $sum: '$finalValue' } } },
    { $sort: { sum: -1 } },
    { $limit: 5 }
])

// Os 1/5 passageiros que gastaram menos em corridas
db.trips.aggregate([
    { $lookup: { from: 'people', localField: 'passenger', foreignField: '_id', as: 'passenger' } },
    { $unwind: '$passenger' },
    { $group: { _id: { cpf: '$passenger.cpf', name: '$passenger.name' }, sum: { $sum: '$finalValue' } } },
    { $sort: { sum: -1 } },
    { $skip: db.people.count({ cnh: { $exists: false } }) * (4 / 5) }
])

// Listar ids dos motoristas com apenas um telefone cadastrado
db.people.find({ cnh: { $exists: true }, phone: { $size: 1 } })

//// Criação de índice de texto
db.people.createIndex({ name: 'text', email: 'text' })

// Consulta por nomes de motoristas com o sobrenome moura
db.people.find({ $text: { $search: 'moura' }, cnh: { $exists: true } }).pretty()


// Retornar o total de documentos da base de dados carservice

db.trips.find().count()

// retornar as viagens com a distância maior que 4
db.trips.find( {distance: { $gte: 4 } } )

// listar motoristas do estado do rio grande do sul apenas, ordenados pela distancia percorrida do motorista
db.trips.aggregate([
    { $match: { 'driver.address.state': 'rio grande do sul' } },
    {
        $group: {
            _id: { name: '$driver.name', state: '$driver.address.state', uuid: '$driver.uuid' },
            distanciaTotal: { $sum: '$distance' }
        }
    },
    { $sort: { distanciaTotal: -1 } }
])

// listar motoristas e seus estados, ordenados pela distancia percorrida 

db.trips.aggregate([
{ $lookup: {from: 'people', localField: 'driver', foreignField: '_id', as:'driver'} },
{$unwind: '$driver'},
{
    $group:{
        _id:{cpf: '$driver.cpf', name: '$driver.name', state: '$driver.address.state'},
        distanciaTotal:{ $sum: '$distance'}
    }
},
    {$sort: { distanciaTotal: -1 } }                         

])

// Retornar as viagens que aconteceram depois do dia 01-04-2015
db.trips.find( {
    date: { $gt: new Date('2015-04-01') },
 } ).pretty()

// Quantas viagens foram realizadas no dia 2023-10-03  
db.trips.find({date: new Date('2023-10-03')}).count()


// #SET : atualizando o nome do estado "santa catarina" pra "santa caratina - SC" APENAS para a
// primeira pessoa que se encaixe na condição
db.people.update(
    {"address.state" : "santa catarina"},
    {
        $set :{
        "address.state" : "santa catarina - SC"
    }
    }
)

// #PUSH : adicionando mais um telefone a um determinado usuário
db.people.update(
    {"_id" : ObjectId("5d21ca0cbb08843b1bfd2b6b")},
    {
        $push :{
        "phone" : "(82) 3333-3333"
    }
    }
)

// #EACH vários updates em sequência
db.people.update(
    {"_id" : ObjectId("5d21ca0cbb08843b1bfd2b6b")},
    {
        $push :{
        phone : {$each : ["(82) 4444-4444", "(82) 4444-2222"]}
    }
    }
)

// #FINDONE : encontrando algum (e apenas um) elemento da coleção trips que tenha o rating maior que 4
db.trips.findOne(
    {
        rating: {$gt: 4}
    }
)

//$SUBSTR: Quebrar o primeiro nome dos motoristas em uma substring
db.trips.aggregate([ 
{ $lookup: {from: 'people', localField: 'driver', foreignField: '_id', as:'driver'} },
{$unwind: '$driver'},
    { $project: { _id: {name: '$driver.name'},
           "subString": { $substrBytes:["$driver.name", 0, 4 ]}
     }
        }
 ])

// $TOUPPER: Retornar o nome dos passageiros em letras maiúsculas
db.trips.aggregate([ 
{ $lookup: { from: 'people', localField: 'passenger', foreignField: '_id', as: 'passenger' } },
{ $unwind: '$passenger'},
{ $project: { _id: { name: { $toUpper: "$passenger.name"} } } }
 ])

// $CONCAT: Nome dos passageiros e seus respectivos emails 
db.trips.aggregate([
    { $lookup: { from: 'people', localField: 'passenger', foreignField: '_id', as: 'passenger' } },
    { $unwind: '$passenger'},
           { $project: { Passengers:{ $concat: [ "$passenger.name", " - ", "$passenger.email" ] } } }
])

// $ALL: Retornar todas as viagens realizadas por veículos que são do ano 2018
db.trips.find({"vehicle.year":{ $all:[2018]}}).pretty()

// SETINTERSECTION 
db.people.aggregate(
    [
      { $project: { name:1, phone: 1, phone:1, Phones: { $setIntersection: [ "$phone", "$phone"] }, _id: 0 } }
    ]
 )

// SETISSUBSET
db.people.aggregate(
    [
      { $project:{name:1, phone: 1, phone:1, Phones: { $setIsSubset: [ "$phone", "$phone"] }, _id:0 } }
    ]
 )

//SETUNION
db.people.aggregate(
   [
     { $project: { phone:1, phone:1, Phones: { $setUnion: [ "$phone", "$phone" ] }, _id: 0 } }
   ]
)

//#OUT: gerando uma nova coleção com o cpf e email das pessoas
db.people.aggregate(
     [
       {$group: { _id: "$cpf", email: {$push: "$email"}}},
       {$out: "emailpeople"}
    ]
)

//#MULTIPLY: consulta para estimar a taxa de duração de tempo de viagem em relação ao valor final
db.trips.aggregate(
    [
      {$project: { _id: 1, inicioCorrida: "$date", valorDistancia: {$multiply: ["$distance", 1.5]}, 
      valorTempo: {$subtract: ["$finalValue", {$multiply: ["$distance", 1.5]}]}
    }
    }
   ]
)


