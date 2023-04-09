class CurrencyAPI {
    fun ratesAccordingToRUB(): Map<Currency, Double> {
        return mapOf(
            Currency.RUB to 1.0,
            Currency.USD to 1.0 / 80,
            Currency.EUR to 1.0 / 90
        )
    }
}