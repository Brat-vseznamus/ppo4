import com.mongodb.rx.client.MongoClients
import io.netty.buffer.ByteBuf
import io.netty.handler.codec.http.HttpMethod
import io.reactivex.netty.protocol.http.server.HttpServer
import io.reactivex.netty.protocol.http.server.HttpServerRequest
import rx.Observable

fun <T> HttpServerRequest<T>.forceGet(name: String): String {
    return queryParameters[name]?.get(0)
        ?: error("Could not get ot parse parameter $name")
}

const val PATH = "/v0"

fun main(args: Array<String>) {
    if (args.size < 2) error("Not enough args")

    val mongoUrl = args[0]
    val mongoDbName = args[1]

    val db = MongoDB(MongoClients.create(mongoUrl).getDatabase(mongoDbName))
    val currencyAPI = CurrencyAPI()

    val server = HttpServer.newServer(8080)

    server.start { req, resp ->
        val obs = try {
            when (req.httpMethod) {
                HttpMethod.PUT -> handlePut(req, db)
                HttpMethod.GET -> handleGet(req, db, currencyAPI)
                else -> error("Unimplemented method")
            }
        } catch (e: Exception) {
            Observable.just("Exception in handling: ${e.message}")
        }
        resp.writeString(obs)
    }

    server.awaitShutdown()
}

private fun handleGet(
    req: HttpServerRequest<ByteBuf>,
    db: MongoDB,
    currencyAPI: CurrencyAPI
): Observable<String>? = when (req.decodedPath) {
    "$PATH/user" -> {
        val id = req.forceGet("id").toInt()

        db.user(id).map { user ->
            "User{" +
                    "id=${user.id}," +
                    "nickname=${user.nickname}," +
                    "currency=${user.currency}" +
            "}"
        }
    }

    "$PATH/item" -> {
        val userId = req.forceGet("userId").toInt()
        val itemId = req.forceGet("itemId").toInt()

        db.user(userId).flatMap { user ->
            db.item(itemId).flatMap { item ->
                Observable.just(
                    "Item{" +
                            "id=${item.id}," +
                            "name=${item.name}," +
                            "price=${item.priceRUB * currencyAPI.ratesAccordingToRUB()[user.currency]!!}" +
                    "}"
                )
            }
        }
    }

    else -> error("Unimplemented path")
}

private fun handlePut(
    req: HttpServerRequest<ByteBuf>,
    db: MongoDB
): Observable<String>? = when (req.decodedPath) {
    "$PATH/user" -> {
        val id = req.forceGet("id").toInt()
        val nickname = req.forceGet("nickname")
        val currency = Currency.valueOf(req.forceGet("currency"))

        val user = User(id, nickname, currency)
        db.addUser(user).map { "USER ADDITION: " + (if (it) "SUCCESS"  else "FAIL") }
    }

    "$PATH/item" -> {
        val id = req.forceGet("id").toInt()
        val name = req.forceGet("name")
        val price = req.forceGet("price").toDouble()

        val item = Item(id, name, price)
        db.addItem(item).map { "ITEM ADDITION: " + (if (it) "SUCCESS"  else "FAIL") }
    }

    else -> error("Unimplemented path")
}