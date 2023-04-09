import com.mongodb.client.model.Filters
import com.mongodb.rx.client.MongoDatabase
import org.bson.Document
import rx.Observable
import rx.Scheduler
import rx.schedulers.Schedulers

interface Database {
    fun item(id: Int): Observable<Item>
    fun user(id: Int): Observable<User>
    fun addItem(item: Item): Observable<Boolean>
    fun addUser(user: User): Observable<Boolean>
}

class MongoDB(private val actualDB : MongoDatabase) : Database {
    private val scheduler: Scheduler = Schedulers.io()


    private fun <T> objectById(id: Int, table: String, create: (doc : Document) -> T): Observable<T> {
        return actualDB.getCollection(table)
            .find(Filters.eq("id", id))
            .toObservable()
            .map(create)
            .subscribeOn(scheduler)
    }

    private fun <T> insertObjectWithId(
        obj: T,
        id: Int,
        table: String,
        getObj: (id: Int) -> Observable<T>,
        parseToEntity: (obj: T) -> Map<String, Any>): Observable<Boolean> {
        return getObj(id)
            .singleOrDefault(null)
            .flatMap { entity ->
                when(entity) {
                    null -> {
                        val newDoc = Document(parseToEntity(obj))
                        actualDB.getCollection(table)
                            .insertOne(newDoc)
                            .asObservable()
                            .isEmpty
                            .map { !it }
                    }
                    else -> Observable.just(false)
                }
            }
    }

    override fun item(id: Int): Observable<Item> {
        return objectById(id, ITEMS) {
            Item(id = id,
                name = it.getString("name"),
                priceRUB = it.getDouble("priceRUB"))
            }
    }

    override fun user(id: Int): Observable<User> {
        return objectById(id, USERS) {
            User(id = id,
                nickname = it.getString("nickname"),
                currency = Currency.valueOf(it.getString("currency"))
            )
        }
    }

    override fun addItem(item: Item): Observable<Boolean> {
        return insertObjectWithId(
            item,
            item.id,
            ITEMS,
            this::item,
        ) {
            mapOf(
                "id" to item.id,
                "name" to item.name,
                "priceRUB" to item.priceRUB
            )
        }
    }

    override fun addUser(user: User): Observable<Boolean> {
        return insertObjectWithId(
            user,
            user.id,
            USERS,
            this::user,
        ) {
            mapOf(
                "id" to user.id,
                "nickname" to user.nickname,
                "currency" to user.currency.name
            )
        }
    }

    companion object {
        private const val ITEMS = "items"
        private const val USERS = "users"
    }
}

