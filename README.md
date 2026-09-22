# BookUrRide

A microservices-based ride-hailing backend built to understand — from the ground up — how apps like Uber, Ola and Rapido handle thousands of concurrent ride requests using **Kafka** for event-driven communication and **Redis Geospatial** for real-time driver matching.

---

## Project Overview

BookUrRide simulates the core backend of a ride-hailing platform, broken into three independent Spring Boot microservices that never call each other directly for the critical path. Instead, they coordinate through **Kafka topics** (asynchronous, durable messaging) and look up live driver positions through **Redis's geospatial commands** (sub-millisecond radius search).

This project was built specifically as a learning exercise to answer two questions:

- **How does Kafka let independent services react to events without knowing about each other, while staying reliable if one service goes down?**
- **How does Redis let a system find "which of these 1000 drivers is nearest to this pickup point" in milliseconds instead of scanning a database table?**

The result is a working, end-to-end simulation of the "request a ride → find a driver → get matched" flow that mirrors, at a small scale, the same architectural patterns production ride-hailing systems use.

---

## Architecture

```
Driver's Phone (every 3s)
        │
        ▼
 Location Service ──── GEOADD ────▶  Redis (drivers:locations)
        ▲
        │  GET /drivers/nearby (REST)
        │
Rider App
   │
   ▼ POST /rides/request
Ride Service ──── produces ────▶  Kafka Topic: ride.requested
                                          │
                                          ▼
                                  Matching Service (consumer)
                                          │
                                          ├──▶ calls Location Service (REST) for nearby drivers
                                          │
                                          ├──▶ scores each driver (distance 70% + rating 30%)
                                          │
                                          ▼
                                  Kafka Topic: ride.matched ──── consumed by ───▶ Ride Service
                                                                                        │
                                                                                        ▼
                                                                          updates MySQL: driverId + status = ACCEPTED
```

**The core design principle:** `ride-service` and `matching-service` never hold a reference to each other's URL for the matching flow — they only know about Kafka topic names. This is what makes the system resilient: if `matching-service` is temporarily down, `ride.requested` events simply queue up in Kafka and get processed the moment it comes back, instead of the request failing outright.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Java 21 |
| Framework | Spring Boot 3.2.0 |
| Messaging | Apache Kafka 4.3.1 (KRaft mode — no ZooKeeper) |
| Caching / Geospatial | Redis 7.4 |
| Database | MySQL 8.4 |
| Service-to-service calls | Spring Cloud OpenFeign |
| Build tool | Maven |
| Infrastructure | Docker Compose |
| ORM | Spring Data JPA / Hibernate |
| Boilerplate reduction | Lombok |

---

## Services

Three independently runnable Spring Boot applications, each with its own `pom.xml`, port, and data store — a genuine microservices split rather than a modularized monolith.

### `location-service` — port `8082`

Tracks every driver's live GPS position in Redis and answers "who's nearby" queries.

- **Owns:** Redis (`drivers:locations` geo-index) — no database of its own
- **Depends on:** nothing else; it's a leaf service
- **Called by:** driver's phone (location pings), `matching-service` (nearby-driver lookups)
- **Core class:** `LocationService.java` — wraps `RedisTemplate.opsForGeo()` for `GEOADD`, `GEORADIUS`, and `ZREM`

### `ride-service` — port `8083`

The system of record for a ride. Owns the entire ride lifecycle from request to completion.

- **Owns:** MySQL `rides` table (via the `Ride` JPA entity)
- **Depends on:** Kafka (both producing and consuming)
- **Called by:** the rider's app directly (REST), and indirectly via Kafka from `matching-service`
- **Core classes:**
  - `RideService.java` — business logic: create ride, publish `RideRequestedEvent`, later update with assigned driver
  - `RideEventConsumer.java` — listens on `ride.matched`, writes the driver assignment back to MySQL
  - `KafkaConfig.java` — declares the `ride.requested` and `ride.matched` topics (3 partitions each)

### `matching-service` — port `8084`

The "brain" of the system — pure orchestration, no database of its own. Consumes a ride request, finds a driver, and hands the result back to Kafka.

- **Owns:** nothing persistent — fully stateless
- **Depends on:** Kafka (consumer + producer) and `location-service` (REST via Feign)
- **Called by:** nobody directly — it's triggered entirely by Kafka messages
- **Core classes:**
  - `RideEventConsumer.java` — listens on `ride.requested`
  - `MatchingService.java` — calls `location-service`, runs the scoring algorithm, publishes `RideMatchedEvent`
  - `LocationServiceClient.java` — declarative Feign client for the outbound REST call

| Service | Port | Responsibility | Stores state? |
|---|---|---|---|
| location-service | 8082 | Real-time driver location tracking & radius search | Redis only |
| ride-service | 8083 | Ride lifecycle, source of truth, Kafka producer + consumer | MySQL |
| matching-service | 8084 | Driver discovery + scoring, Kafka consumer + producer | Stateless |

---

## Key Concepts Demonstrated

- **Kafka event-driven architecture** — topics, producers, consumers, consumer groups, message keys, serialization/deserialization with JSON
- **Redis Geospatial commands** — `GEOADD`, `GEORADIUS` (via Spring Data Redis's `opsForGeo()`), `ZREM`, `GEOPOS`, `GEODIST`
- **Asynchronous vs. synchronous communication** — Kafka (async, durable, decoupled) used for the ride-matching handoff; REST/Feign (sync, blocking) used for the location lookup
- **Ride state machine** — `REQUESTED → MATCHING → ACCEPTED → DRIVER_ARRIVING → RIDE_STARTED → COMPLETED` (with `CANCELLED` reachable from multiple states)
- **Driver scoring algorithm** — weighted score combining proximity (70%) and rating (30%) to pick the best available driver
- **Service-to-service REST communication** — declarative HTTP client via `@FeignClient`
- **Infrastructure as code** — single `docker-compose.yml` spinning up Redis, MySQL, and Kafka (KRaft-mode, no ZooKeeper) for local development

---

## Project Structure

```
BookUrRide/
├── docker-compose.yml
├── location-service/
│   └── src/main/java/com/rideshare/locationservice/
│       ├── config/RedisConfig.java
│       ├── controller/LocationController.java
│       ├── service/LocationService.java
│       └── dto/ (DriverLocationRequest, NearByDriverResponse)
├── ride-service/
│   └── src/main/java/com/rideshare/rideservice/
│       ├── config/KafkaConfig.java
│       ├── controller/RideController.java
│       ├── service/RideService.java
│       ├── service/RideEventConsumer.java
│       ├── model/ (Ride, RideStatus)
│       ├── event/ (RideRequestedEvent, RideMatchedEvent)
│       └── dto/ (RideRequest, RideResponse)
└── matching-service/
    └── src/main/java/com/rideshare/matchingservice/
        ├── client/LocationServiceClient.java
        ├── service/MatchingService.java
        ├── service/RideEventConsumer.java
        ├── event/ (RideRequestedEvent, RideMatchedEvent)
        └── dto/NearByDriverResponse.java
```

---

## API Reference

### location-service — `:8082`

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/v1/locations/drivers/update` | Update a driver's current location (called every ~3s by the driver's phone) |
| `GET` | `/api/v1/locations/drivers/nearby?latitude=&longitude=&radius=` | Get up to 10 nearest drivers within a radius (km), sorted by distance |
| `DELETE` | `/api/v1/locations/drivers/{driverId}` | Remove a driver from the location index (goes offline) |

### ride-service — `:8083`

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/v1/rides/request` | Rider requests a new ride; saves it and publishes `RideRequestedEvent` |
| `GET` | `/api/v1/rides/{rideId}` | Get current ride status/details |
| `GET` | `/api/v1/rides/rider/{riderId}` | Get a rider's full ride history |
| `PUT` | `/api/v1/rides/{rideId}/start` | Driver starts the ride |
| `PUT` | `/api/v1/rides/{rideId}/complete` | Mark the ride as completed |
| `PUT` | `/api/v1/rides/{rideId}/cancel` | Cancel the ride |

### matching-service — `:8084`

The matching-service has no public REST API of its own. It acts as a Kafka consumer/producer and makes an outbound REST call to `location-service` to find nearby drivers.


