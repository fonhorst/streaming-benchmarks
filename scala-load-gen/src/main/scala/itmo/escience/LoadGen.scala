package itmo.escience

import java.util.Random

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


object LoadGen {

  val CAMPAIGNS = "campaigns"

  val AD_TYPES = List("banner", "modal", "sponsored-search", "mail", "mobile")

//  val EVENT_TYPES = List("view", "click", "purchase")
  val EVENT_TYPES = List("view")

  val rand = new Random(System.currentTimeMillis())

  var numCampaigns = 100

  var redisHost = "127.0.0.1"

  //var kafkaHosts = "192.168.92.72:9092,192.168.92.73:9092"
  var kafkaHosts = "127.0.0.1:9092"

  var kafkaTopic = "ad-events"


  def main(args:Array[String]) = {
    val throughput = args(0).toInt
    doNewSetup(redisHost)
    run(throughput = throughput)
  }

  def doNewSetup(redisHost: String) = {
    println("Writing campaigns data to Redis.")

    val campaigns = makeIds(numCampaigns)

    val redis = new Jedis(redisHost)
    redis.flushAll()
    for(campaign <- campaigns) {
      redis.sadd("campaigns", campaign)
    }
  }

  def run(throughput:Long) ={
    println(s"Running, emitting $throughput tuples per second.")

    val ads = genAds(redisHost)

    val pageIds = makeIds(100)
    val userIds = makeIds(100)
    val startTimeNS = System.currentTimeMillis() * 1000000
    val periodNS = 1000000000 / throughput
    val timesIter = Stream.from(0).map(x => periodNS * x + startTimeNS)

    val props = Map[String, Object]("bootstrap.servers" -> kafkaHosts)

    val producer = new KafkaProducer[String, String](props, new StringSerializer, new StringSerializer)

    try {
      for((t, i) <- timesIter.zipWithIndex){
        val cur = System.currentTimeMillis()
        val tms = t / 1000000

        if (tms > cur) {
          Thread.sleep(tms - cur)
          Future {
            val newcur = System.currentTimeMillis()
            if (newcur > tms + 100) {
              println(s"Falling behind by: ${newcur - tms} ms")
            }
          }
        }

        producer.send(new ProducerRecord(kafkaTopic, makeKafkaEventsAt(t, ads, userIds, pageIds)))
        if (i % 1000 == 0){
          Future {
            println(s"Emitted $i records")
          }
        }
      }
    } catch {
      case e:Exception => throw e
    } finally {
      producer.close()
    }
  }

  def makeIds(n: Int): List[String] = {
    (0 to n).map(x => java.util.UUID.randomUUID().toString).toList
  }

  def genAds(redisHost:String) = {
    val redis = new Jedis(redisHost)

    val camapaigns = redis.smembers(CAMPAIGNS)

    val ads = makeIds(10 * numCampaigns)

    val partitions = (0 to (ads.size / 10)).map(i => ads.slice(i, i * 10))
    val campaignsAds = camapaigns.zip(partitions)

    if (camapaigns.size() < numCampaigns) {
      throw new RuntimeException("No Campaigns found. Please run with -n first.")
    }

    val campaignsPipe = redis.pipelined()
    //campaignsPipe.multi()
    for((campaign, ads) <- campaignsAds){
      ads.foreach(campaignsPipe.set(_, campaign))
    }
    campaignsPipe.sync()

    ads
  }

  def randNth(ids: List[String]) = {
    ids(rand.nextInt(ids.size))
  }

  def makeKafkaEventsAt(t: Long, ads: List[String], userIds: List[String], pageIds: List[String]): String = {
    val str = s"""{
        "user_id": ${randNth(userIds)},
        "page_id": ${randNth(pageIds)},
        "ad_id": ${randNth(ads)},
        "ad_type": ${randNth(AD_TYPES)},
        "event_type": ${randNth(EVENT_TYPES)},
        "event_time": $t,
        "ip_address": "1.2.3.4"
    }""".stripMargin

    str
  }
}