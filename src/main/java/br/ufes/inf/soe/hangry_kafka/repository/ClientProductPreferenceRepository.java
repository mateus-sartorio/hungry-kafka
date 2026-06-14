package br.ufes.inf.soe.hangry_kafka.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import br.ufes.inf.soe.hangry_kafka.entity.ClientProductPreference;
import br.ufes.inf.soe.hangry_kafka.entity.ClientProductPreferenceId;

@Repository
public interface ClientProductPreferenceRepository extends JpaRepository<ClientProductPreference, ClientProductPreferenceId> {
}
