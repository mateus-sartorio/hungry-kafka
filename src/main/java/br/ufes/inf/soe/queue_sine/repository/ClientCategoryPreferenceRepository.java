package br.ufes.inf.soe.queue_sine.repository;

import br.ufes.inf.soe.queue_sine.entity.ClientCategoryPreference;
import br.ufes.inf.soe.queue_sine.entity.ClientCategoryPreferenceId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ClientCategoryPreferenceRepository extends JpaRepository<ClientCategoryPreference, ClientCategoryPreferenceId> {
}
