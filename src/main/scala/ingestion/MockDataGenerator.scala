package ingestion

import io.circe.generic.auto._
import io.circe.syntax._

import java.time.Instant
import java.util.UUID
import scala.util.Random

/** Realistic mock data generator for e-commerce transactions, clients, and products. */
object MockDataGenerator {

  private val random = new Random()

  // --- Reference data ---

  private val firstNames = Array(
    "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
    "David", "Elizabeth", "William", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Charles", "Karen", "Emma", "Oliver", "Ava", "Liam", "Sophia",
    "Noah", "Isabella", "Ethan", "Mia", "Lucas", "Charlotte", "Mason", "Amelia",
    "Logan", "Harper", "Alexander", "Evelyn", "Aiden", "Abigail", "Jackson"
  )

  private val lastNames = Array(
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson"
  )

  private val countries = Array(
    "United States", "United Kingdom", "Canada", "Germany", "France",
    "Australia", "Japan", "Brazil", "India", "Mexico",
    "Spain", "Italy", "Netherlands", "Sweden", "South Korea"
  )

  private val citiesByCountry: Map[String, Array[String]] = Map(
    "United States"  -> Array("New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Seattle"),
    "United Kingdom" -> Array("London", "Manchester", "Birmingham", "Leeds", "Glasgow"),
    "Canada"         -> Array("Toronto", "Vancouver", "Montreal", "Calgary", "Ottawa"),
    "Germany"        -> Array("Berlin", "Munich", "Hamburg", "Frankfurt", "Cologne"),
    "France"         -> Array("Paris", "Lyon", "Marseille", "Toulouse", "Nice"),
    "Australia"      -> Array("Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide"),
    "Japan"          -> Array("Tokyo", "Osaka", "Yokohama", "Nagoya", "Sapporo"),
    "Brazil"         -> Array("São Paulo", "Rio de Janeiro", "Brasília", "Salvador", "Fortaleza"),
    "India"          -> Array("Mumbai", "Delhi", "Bangalore", "Chennai", "Kolkata"),
    "Mexico"         -> Array("Mexico City", "Guadalajara", "Monterrey", "Puebla", "Cancún"),
    "Spain"          -> Array("Madrid", "Barcelona", "Valencia", "Seville", "Bilbao"),
    "Italy"          -> Array("Rome", "Milan", "Naples", "Turin", "Florence"),
    "Netherlands"    -> Array("Amsterdam", "Rotterdam", "The Hague", "Utrecht", "Eindhoven"),
    "Sweden"         -> Array("Stockholm", "Gothenburg", "Malmö", "Uppsala", "Linköping"),
    "South Korea"    -> Array("Seoul", "Busan", "Incheon", "Daegu", "Daejeon")
  )

  private val segments = Array("Premium", "Standard", "Budget", "Enterprise", "VIP")

  private val categories: Map[String, Array[String]] = Map(
    "Electronics"     -> Array("Smartphones", "Laptops", "Tablets", "Headphones", "Cameras"),
    "Clothing"        -> Array("T-Shirts", "Jeans", "Dresses", "Jackets", "Shoes"),
    "Home & Garden"   -> Array("Furniture", "Lighting", "Kitchenware", "Bedding", "Decor"),
    "Sports"          -> Array("Running Shoes", "Yoga Mats", "Weights", "Bicycles", "Jerseys"),
    "Books"           -> Array("Fiction", "Non-Fiction", "Comics", "Textbooks", "Audiobooks"),
    "Beauty"          -> Array("Skincare", "Makeup", "Fragrances", "Haircare", "Nail Care"),
    "Food & Beverage" -> Array("Coffee", "Tea", "Snacks", "Supplements", "Organic"),
    "Toys"            -> Array("Action Figures", "Board Games", "Puzzles", "Dolls", "LEGO")
  )

  private val brands = Array(
    "TechNova", "UrbanStyle", "HomeComfort", "SportsPro", "BookWorm",
    "GlowUp", "NutriPlus", "FunFactory", "EcoSmart", "PrimeLine",
    "VoltEdge", "AquaPure", "ZenithGear", "NexGen", "Artisan Co"
  )

  private val paymentMethods = Array("credit_card", "debit_card", "paypal", "bank_transfer", "crypto")
  private val statuses = Array("completed", "completed", "completed", "completed", "pending", "refunded")
  private val currencies = Array("USD", "USD", "USD", "EUR", "GBP", "CAD", "JPY")

  // --- Case classes ---

  case class Transaction(
    transaction_id: String,
    client_id: String,
    product_id: String,
    quantity: Int,
    unit_price: Double,
    total_amount: Double,
    currency: String,
    payment_method: String,
    status: String,
    transaction_time: String
  )

  case class Client(
    client_id: String,
    first_name: String,
    last_name: String,
    email: String,
    country: String,
    city: String,
    age: Int,
    segment: String,
    registered_at: String
  )

  case class Product(
    product_id: String,
    name: String,
    category: String,
    sub_category: String,
    brand: String,
    price: Double,
    weight_kg: Double,
    rating: Double,
    stock: Int
  )

  // --- Generators ---

  private def pick[T](arr: Array[T]): T = arr(random.nextInt(arr.length))

  private def round2(d: Double): Double = math.round(d * 100.0) / 100.0

  /** Generate a pool of client IDs for use in transactions. */
  def generateClients(count: Int): Seq[Client] = {
    (1 to count).map { _ =>
      val id = s"CLI-${UUID.randomUUID().toString.take(8)}"
      val firstName = pick(firstNames)
      val lastName = pick(lastNames)
      val country = pick(countries)
      val city = pick(citiesByCountry.getOrElse(country, Array("Unknown")))
      val email = s"${firstName.toLowerCase}.${lastName.toLowerCase}${random.nextInt(999)}@example.com"
      val daysAgo = random.nextInt(1825) // up to 5 years ago
      val registeredAt = Instant.now().minusSeconds(daysAgo.toLong * 86400).toString

      Client(
        client_id = id,
        first_name = firstName,
        last_name = lastName,
        email = email,
        country = country,
        city = city,
        age = 18 + random.nextInt(62),
        segment = pick(segments),
        registered_at = registeredAt
      )
    }
  }

  /** Generate a pool of products for use in transactions. */
  def generateProducts(count: Int): Seq[Product] = {
    (1 to count).map { _ =>
      val id = s"PRD-${UUID.randomUUID().toString.take(8)}"
      val category = pick(categories.keys.toArray)
      val subCategory = pick(categories(category))
      val brand = pick(brands)
      val basePrice = category match {
        case "Electronics"   => 50.0 + random.nextDouble() * 1950.0
        case "Clothing"      => 10.0 + random.nextDouble() * 290.0
        case "Home & Garden" => 20.0 + random.nextDouble() * 980.0
        case "Sports"        => 15.0 + random.nextDouble() * 485.0
        case "Books"         => 5.0 + random.nextDouble() * 95.0
        case "Beauty"        => 8.0 + random.nextDouble() * 192.0
        case "Food & Beverage" => 3.0 + random.nextDouble() * 97.0
        case "Toys"          => 5.0 + random.nextDouble() * 195.0
        case _               => 10.0 + random.nextDouble() * 490.0
      }

      Product(
        product_id = id,
        name = s"$brand $subCategory ${random.nextInt(9000) + 1000}",
        category = category,
        sub_category = subCategory,
        brand = brand,
        price = round2(basePrice),
        weight_kg = round2(0.1 + random.nextDouble() * 29.9),
        rating = round2(1.0 + random.nextDouble() * 4.0),
        stock = random.nextInt(5000)
      )
    }
  }

  /** Generate a single transaction using existing client and product pools. */
  def generateTransaction(clientIds: Seq[String], productIds: Seq[String]): Transaction = {
    val clientId = clientIds(random.nextInt(clientIds.size))
    val productId = productIds(random.nextInt(productIds.size))
    val quantity = 1 + random.nextInt(10)
    val unitPrice = round2(5.0 + random.nextDouble() * 995.0)
    val total = round2(quantity * unitPrice)
    val secsAgo = random.nextInt(86400 * 7) // up to 7 days ago

    Transaction(
      transaction_id = s"TXN-${UUID.randomUUID().toString.take(12)}",
      client_id = clientId,
      product_id = productId,
      quantity = quantity,
      unit_price = unitPrice,
      total_amount = total,
      currency = pick(currencies),
      payment_method = pick(paymentMethods),
      status = pick(statuses),
      transaction_time = Instant.now().minusSeconds(secsAgo.toLong).toString
    )
  }

  // --- JSON serialization ---

  def transactionToJson(t: Transaction): String = t.asJson.noSpaces
  def clientToJson(c: Client): String = c.asJson.noSpaces
  def productToJson(p: Product): String = p.asJson.noSpaces
}
