package br.ufes.inf.soe.hangry_kafka.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import br.ufes.inf.soe.hangry_kafka.entity.ClientCategoryPreference;
import br.ufes.inf.soe.hangry_kafka.entity.ClientCategoryPreferenceId;

@Repository
public interface ClientCategoryPreferenceRepository extends JpaRepository<ClientCategoryPreference, ClientCategoryPreferenceId> {
}
