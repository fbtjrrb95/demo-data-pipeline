plugins {
    kotlin("jvm") version "2.0.0"                  // Kotlin 2.0 이상: JVM 21 지원
    kotlin("plugin.spring") version "2.0.0"        // Spring 관련 클래스 자동 open 처리
    id("org.springframework.boot") version "3.3.3"
    id("io.spring.dependency-management") version "1.1.6"
}

group = "me.skrew.data"
version = "0.1.0-SNAPSHOT"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21)) // Gradle이 JDK 21로 빌드
    }
}

kotlin {
    jvmToolchain(21)  // Kotlin 컴파일러도 JVM 21용 bytecode 생성
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.jetbrains.kotlin:kotlin-stdlib")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("io.github.microutils:kotlin-logging-jvm:3.0.5")

    implementation("org.springframework.boot:spring-boot-starter")
    implementation("org.springframework.kafka:spring-kafka")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")
    implementation("com.fasterxml.jackson.core:jackson-databind")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("org.springframework.data:spring-data-elasticsearch:5.3.3")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
}
