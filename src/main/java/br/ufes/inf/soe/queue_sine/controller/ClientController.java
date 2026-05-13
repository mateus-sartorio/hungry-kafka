package br.ufes.inf.soe.queue_sine.controller;

import br.ufes.inf.soe.queue_sine.dto.CreateClientRequest;
import br.ufes.inf.soe.queue_sine.entity.Client;
import br.ufes.inf.soe.queue_sine.repository.ClientRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URI;
import java.util.Optional;

@RestController
@RequestMapping("/api/clients")
public class ClientController {

    private final ClientRepository clientRepository;

    public ClientController(ClientRepository clientRepository) {
        this.clientRepository = clientRepository;
    }

    @PostMapping
    public ResponseEntity<Client> createClient(@RequestBody CreateClientRequest req) {
        Client client = new Client();
        client.setName(req.getName());
        Client saved = clientRepository.save(client);
        return ResponseEntity.created(URI.create("/api/clients/" + saved.getId())).body(saved);
    }

    @GetMapping("/{id}")
    public ResponseEntity<Client> getClient(@PathVariable Integer id) {
        Optional<Client> found = clientRepository.findById(id);
        return found.map(ResponseEntity::ok).orElseGet(() -> ResponseEntity.notFound().build());
    }
}
