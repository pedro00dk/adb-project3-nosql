// use carservice
db = db.getSiblingDB('carservice')

// Adicionar endereço de uma pessoa
const name = 'rita da conceição'
// before
db.people.find({ name })
db.people.updateOne({ name }, { $set: { 'address.state': 'pernambuco' } })
// after
db.people.find({ name })

// corrigir valor estimado dos das corridas com valores estimados maior que 20 atuais em 5%
db.trips.update({ estimatedValue: { $gt: 20 } }, { $mul: { estimatedPrice: 1.05 } })

// atualizando um destino utilizando save
db.trips.save({
        "pickupAddress" : {
                "state" : "pernambuco",
                "city" : " recife",
                "street" : "380 av general polidoro"
        },
        "destinationAddress" : {
                "state" : "paraiba",
                "city" : "sao joao do rio do peixe",
                "street" : "19 rua edite ferreira"
        },
        "distance" : 8.391662323696502,
        "estimatedValue" : 14.087493485544753,
        "finalValue" : 15.600647495614673,
        "date" :"2012-10-20",
        "driver" : ObjectId("5d227be0181cd090fb127224"),
        "passenger" : ObjectId("5d227be0181cd090fb127ac5"),
        "vehicle" : {
                "brand" : "toyota",
                "model" : "etios",
                "year" : 2018,
                "color" : "white"
        },
        "payment" : {
                "method" : "credit",
                "tip" : 5
        },
        "rating" : 3
})
