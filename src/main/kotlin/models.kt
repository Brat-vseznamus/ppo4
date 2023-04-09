data class Item(val id: Int, val name: String, var priceRUB : Double)
data class User(val id: Int, val nickname: String, var currency: Currency)

enum class Currency {
    RUB, USD, EUR;
}

