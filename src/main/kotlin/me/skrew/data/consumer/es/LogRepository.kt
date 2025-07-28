package me.skrew.data.consumer.es

import org.springframework.data.elasticsearch.repository.ElasticsearchRepository
import org.springframework.stereotype.Repository

@Repository
interface LogRepository : ElasticsearchRepository<Log?, Long?>
