package br.ufes.inf.soe.queue_sine.controller;

import br.ufes.inf.soe.queue_sine.entity.Product;
import br.ufes.inf.soe.queue_sine.repository.ProductRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/products")
public class ProductController {

    private final ProductRepository productRepository;

    public ProductController(ProductRepository productRepository) {
        this.productRepository = productRepository;
    }

    @GetMapping("/{userId}")
    public List<Product> listProducts(@PathVariable("userId") String userId) {
        return productRepository.findAll();
    }
}
