package br.ufes.inf.soe.hangry_kafka.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import br.ufes.inf.soe.hangry_kafka.dto.ProductResponse;
import br.ufes.inf.soe.hangry_kafka.entity.Client;
import br.ufes.inf.soe.hangry_kafka.entity.ClientCategoryPreference;
import br.ufes.inf.soe.hangry_kafka.entity.ClientProductPreference;
import br.ufes.inf.soe.hangry_kafka.entity.Product;
import br.ufes.inf.soe.hangry_kafka.repository.ClientRepository;
import br.ufes.inf.soe.hangry_kafka.repository.ProductRepository;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collections;

@RestController
@RequestMapping("/products")
public class ProductController {

    private final ProductRepository productRepository;
    private final ClientRepository clientRepository;

    public ProductController(ProductRepository productRepository, ClientRepository clientRepository) {
        this.productRepository = productRepository;
        this.clientRepository = clientRepository;
    }

    @GetMapping("/{clientId}")
    public List<ProductResponse> listProducts(@PathVariable("clientId") String clientIdStr) {
        Integer clientId;
        try {
            clientId = Integer.valueOf(clientIdStr);
        } catch (NumberFormatException e) {
            clientId = -1;
        }

        Client client = clientRepository.findById(clientId).orElse(null);

        Map<Integer, ClientProductPreference> productPrefs = (client != null && client.getProductPreferences() != null)
                ? client.getProductPreferences()
                : Collections.emptyMap();
        Map<Integer, ClientCategoryPreference> categoryPrefs = (client != null
                && client.getCategoryPreferences() != null)
                        ? client.getCategoryPreferences()
                        : Collections.emptyMap();

        List<Product> products = productRepository.findAll();

        return products.stream()
                .map(product -> {
                    float productPriority = 1.0f;
                    ClientProductPreference pPref = productPrefs.get(product.getId());
                    if (pPref != null && pPref.getValue() != null) {
                        productPriority = pPref.getValue();
                    }

                    float categoryPriority = 1.0f;
                    if (product.getCategory() != null) {
                        ClientCategoryPreference cPref = categoryPrefs.get(product.getCategory().getId());
                        if (cPref != null && cPref.getValue() != null) {
                            categoryPriority = cPref.getValue();
                        }
                    }

                    float totalPriority = productPriority * categoryPriority;
                    return new SimpleEntry<>(product, totalPriority);
                })
                .sorted((e1, e2) -> Float.compare(e2.getValue(), e1.getValue()))
                .map(entry -> {
                    Product p = entry.getKey();
                    return new ProductResponse(
                            p.getId(),
                            p.getName(),
                            p.getDescription(),
                            p.getPrice(),
                            p.getPhotoUrl(),
                            entry.getValue());
                })
                .collect(Collectors.toList());
    }
}
