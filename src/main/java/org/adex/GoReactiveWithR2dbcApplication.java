package org.adex;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.annotation.Id;
import org.springframework.data.r2dbc.connectionfactory.init.CompositeDatabasePopulator;
import org.springframework.data.r2dbc.connectionfactory.init.ConnectionFactoryInitializer;
import org.springframework.data.r2dbc.connectionfactory.init.ResourceDatabasePopulator;
import org.springframework.data.r2dbc.core.DatabaseClient;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.relational.core.query.CriteriaDefinition;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;

import io.r2dbc.spi.ConnectionFactory;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.springframework.web.reactive.function.server.RouterFunctions.route;
import static org.springframework.web.reactive.function.server.ServerResponse.ok;

import java.util.Map;

@SpringBootApplication
public class GoReactiveWithR2dbcApplication {

	public static void main(String[] args) {
		SpringApplication.run(GoReactiveWithR2dbcApplication.class, args);
	}

	@Bean
	public ConnectionFactoryInitializer initializer(ConnectionFactory connectionFactory) {
		ConnectionFactoryInitializer initializer = new ConnectionFactoryInitializer();
		initializer.setConnectionFactory(connectionFactory);
		CompositeDatabasePopulator populator = new CompositeDatabasePopulator();
		populator.addPopulators(new ResourceDatabasePopulator(new ClassPathResource("schema.sql")));
		initializer.setDatabasePopulator(populator);
		return initializer;
	}

	@Bean
	public RouterFunction<ServerResponse> routes(RequestHandlerFaçade requestHandler) {
		return route().GET("/repo", requestHandler::fetchAllViaRepository).GET("/dao", requestHandler::fetchAllViaDAO)
				.GET("/dao/{id}", requestHandler::fetchByIdViaDAO).build();
	}

}

@Component
@RequiredArgsConstructor
class RequestHandlerFaçade {

	private final UserRepository userRepository;
	private final UserDao userDao;

	public Mono<ServerResponse> fetchAllViaRepository(ServerRequest serverRequest) {
		return ok().body(this.userRepository.findAll(), User.class);
	}

	public Mono<ServerResponse> fetchAllViaDAO(ServerRequest serverRequest) {
		return ok().body(this.userDao.getAll(), User.class);
	}

	public Mono<ServerResponse> fetchByIdViaDAO(ServerRequest serverRequest) {
		return ok().body(this.userDao.getById(Integer.valueOf(serverRequest.pathVariable("id"))), User.class);
	}
}

@Component
@RequiredArgsConstructor
@Log4j2
class Runner {

	private final UserRepository dao;
	private final UserDao userDao;

	@EventListener(ApplicationReadyEvent.class)
	public void init() {
		this.userDao.deleteAll()
				.thenMany(
						Flux.just("Med", "John", "Maria").map(name -> new User(null, name)).flatMap(this.userDao::save));
		this.userDao.getAll().subscribe(log::info);
	}

}

@Data
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
@Builder
@Table("users")
class User {
	@Id
	private Integer id;
	private String name;
}

interface UserRepository extends ReactiveCrudRepository<User, Integer> {
}

@Component
@RequiredArgsConstructor
class UserDao {

	private final DatabaseClient databaseClient;

	public Flux<User> getAll() {
		return this.databaseClient.select().from(User.class).fetch().all();
	}

	public Mono<User> getById(int id) {
		return this.databaseClient.select().from(User.class).matching(Criteria.where("id").is(id)).fetch().one();
	}

	public Mono<User> save(User user) {
		return this.databaseClient.insert().into(User.class).using(user).fetch().one().map(UserDao::maptoUser);
	}

	public Mono<Integer> deleteAll() {
		return this.databaseClient.delete().from(User.class).fetch().rowsUpdated();
	}

	private static User maptoUser(Map<String, Object> data) {
		System.out.println(data);
		return User.builder().id((Integer) data.get("id")).name(data.get("name").toString()).build();
	}
}