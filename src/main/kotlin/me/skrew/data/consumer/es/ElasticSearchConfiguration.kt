package me.skrew.data.consumer.es

import co.elastic.clients.elasticsearch.ElasticsearchClient
import co.elastic.clients.json.jackson.JacksonJsonpMapper
import co.elastic.clients.transport.rest_client.RestClientTransport
import org.apache.http.HttpHost
import org.elasticsearch.client.RestClient
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.elasticsearch.repository.config.EnableElasticsearchRepositories

@Configuration
@EnableElasticsearchRepositories
open class ElasticSearchConfiguration {
    @Value("\${elasticsearch.hostname}")
    private val hostname: String? = null

    @Value("\${elasticsearch.port}")
    private val port = 0

    @Value("\${elasticsearch.scheme}")
    private val scheme: String? = null

    @Bean
    open fun elasticsearchClient(): ElasticsearchClient {
        val restClient = RestClient.builder(HttpHost(hostname, port, scheme)).build()
        val transport = RestClientTransport(restClient, JacksonJsonpMapper())
        return ElasticsearchClient(transport)
    }
}
