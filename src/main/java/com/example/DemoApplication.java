package com.example;

import java.time.Duration;
import java.util.Random;
import java.util.stream.Stream;

import io.reactivex.Flowable;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.mongodb.core.CollectionOptions;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.repository.InfiniteStream;
import org.springframework.data.mongodb.repository.config.EnableReactiveMongoRepositories;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

@SpringBootApplication
@EnableReactiveMongoRepositories(considerNestedRepositories = true)
@RequiredArgsConstructor
public class DemoApplication implements CommandLineRunner{

	final ReactivePersonRepository repository;
	final MongoOperations ops;

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);
	}

	@Override
	public void run(String... strings) throws Exception {

		ops.createCollection(Person.class, new CollectionOptions(1000, 1000, true));

		Random random = new Random();
		String[] names = {"Svante", "Tom", "Dennis", "Arno", "Oliver (not Gierke)"};

		Stream<Person> personStream = Stream.generate(() -> names[random.nextInt(names.length)])
				.map(Person::new);

		Flux.interval(Duration.ofSeconds(1))
				.zipWith(Flux.fromStream(personStream))
				.map(Tuple2::getT2)
				.flatMap(repository::save)
				.doOnNext(System.out::println)
				.subscribe();
	}

	interface ReactivePersonRepository extends ReactiveCrudRepository<Person,String> {

		Flowable<Person> findBy();

		@InfiniteStream
		Flowable<Person> streamAllBy();
	}

	@RestController
	@RequiredArgsConstructor
	static class PersonController {

		final ReactivePersonRepository repository;

		@GetMapping
		Flowable<Person> getPeople() {
			return repository.findBy();
		}

		@GetMapping(value = "stream",
				produces = MediaType.TEXT_EVENT_STREAM_VALUE)
		Flowable<Person> stream() {
			return repository.streamAllBy();
		}

	}

	@Data
	static class Person {

		String id;
		final String name;
	}
}
