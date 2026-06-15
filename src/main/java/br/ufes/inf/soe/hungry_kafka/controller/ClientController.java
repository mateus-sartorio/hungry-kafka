package br.ufes.inf.soe.hungry_kafka.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import br.ufes.inf.soe.hungry_kafka.dto.CreateClientRequest;
import br.ufes.inf.soe.hungry_kafka.entity.Client;
import br.ufes.inf.soe.hungry_kafka.repository.ClientRepository;

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

    @PatchMapping("/{id}")
    public ResponseEntity<Client> updateClient(@PathVariable Integer id, @RequestBody CreateClientRequest req) {
        Optional<Client> found = clientRepository.findById(id);
        if (found.isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        Client client = found.get();
        client.setName(req.getName());
        Client saved = clientRepository.save(client);
        return ResponseEntity.ok(saved);
    }
}
