package br.ufes.inf.soe.hangry_kafka.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import br.ufes.inf.soe.hangry_kafka.entity.Client;

@Repository
public interface ClientRepository extends JpaRepository<Client, Integer> {

}
