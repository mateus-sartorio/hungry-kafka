package br.ufes.inf.soe.queue_sine.repository;

import br.ufes.inf.soe.queue_sine.entity.ClientProductPreference;
import br.ufes.inf.soe.queue_sine.entity.ClientProductPreferenceId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ClientProductPreferenceRepository extends JpaRepository<ClientProductPreference, ClientProductPreferenceId> {
}
