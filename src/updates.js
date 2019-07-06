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
