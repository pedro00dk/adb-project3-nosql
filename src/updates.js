// use carservice
db = db.getSiblingDB('carservice')

// Adicionar endereço de uma pessoa
const name = 'rita da conceição'
// before
db.people.find({ $text: { $search: `"${name}"` } })
db.people.update({ $text: { $search: `"${name}"` } }, { $set: { 'address.state': 'pernambuco' } })
// after
db.people.find({ $text: { $search: `"${name}"` } })
