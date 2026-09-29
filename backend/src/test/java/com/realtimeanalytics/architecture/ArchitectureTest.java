package com.realtimeanalytics.architecture;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

@AnalyzeClasses(packages = "com.realtimeanalytics", importOptions = ImportOption.DoNotIncludeTests.class)
public class ArchitectureTest {

    private static final String COMMON = "..common..";
    private static final String SECURITY = "..security..";
    private static final String USER = "..user..";
    private static final String CATALOG = "..catalog..";
    private static final String INVENTORY = "..inventory..";
    private static final String CART = "..cart..";
    private static final String ORDER = "..order..";
    private static final String PAYMENT = "..payment..";
    private static final String OUTBOX = "..outbox..";

    @ArchTest
    static final ArchRule common_should_not_depend_on_other_modules =
            noClasses().that().resideInAPackage(COMMON)
                    .should().dependOnClassesThat().resideInAnyPackage(
                            SECURITY, USER, CATALOG, INVENTORY, CART, ORDER, PAYMENT, OUTBOX
                    );

    @ArchTest
    static final ArchRule user_should_only_depend_on_common =
            noClasses().that().resideInAPackage(USER)
                    .should().dependOnClassesThat().resideInAnyPackage(
                            SECURITY, CATALOG, INVENTORY, CART, ORDER, PAYMENT, OUTBOX
                    );

    @ArchTest
    static final ArchRule catalog_should_only_depend_on_common =
            noClasses().that().resideInAPackage(CATALOG)
                    .should().dependOnClassesThat().resideInAnyPackage(
                            SECURITY, USER, INVENTORY, CART, ORDER, PAYMENT, OUTBOX
                    );

    @ArchTest
    static final ArchRule inventory_should_not_depend_on_unrelated_modules =
            noClasses().that().resideInAPackage(INVENTORY)
                    .should().dependOnClassesThat().resideInAnyPackage(
                            SECURITY, USER, CART, ORDER, PAYMENT, OUTBOX
                    );

    @ArchTest
    static final ArchRule cart_should_not_depend_on_unrelated_modules =
            noClasses().that().resideInAPackage(CART)
                    .should().dependOnClassesThat().resideInAnyPackage(
                            SECURITY, INVENTORY, ORDER, PAYMENT, OUTBOX
                    );

    @ArchTest
    static final ArchRule order_should_not_depend_on_unrelated_modules =
            noClasses().that().resideInAPackage(ORDER)
                    .should().dependOnClassesThat().resideInAnyPackage(
                            SECURITY, CART, PAYMENT
                    );

    @ArchTest
    static final ArchRule payment_should_not_depend_on_unrelated_modules =
            noClasses().that().resideInAPackage(PAYMENT)
                    .should().dependOnClassesThat().resideInAnyPackage(
                            SECURITY, USER, CATALOG, INVENTORY, CART
                    );

    @ArchTest
    static final ArchRule outbox_should_not_depend_on_domain_modules =
            noClasses().that().resideInAPackage(OUTBOX)
                    .should().dependOnClassesThat().resideInAnyPackage(
                            SECURITY, USER, CATALOG, INVENTORY, CART, ORDER, PAYMENT
                    );

    @ArchTest
    static final ArchRule security_should_only_depend_on_common_and_user =
            noClasses().that().resideInAPackage(SECURITY)
                    .should().dependOnClassesThat().resideInAnyPackage(
                            CATALOG, INVENTORY, CART, ORDER, PAYMENT, OUTBOX
                    );

    // Cross-module boundaries for internals (repository, entity, infrastructure)
    private static ArchRule moduleInternalsRule(String moduleName) {
        return noClasses().that().resideOutsideOfPackage(".." + moduleName + "..")
                .should().dependOnClassesThat().resideInAnyPackage(
                        ".." + moduleName + ".repository..",
                        ".." + moduleName + ".entity..",
                        ".." + moduleName + ".infrastructure.."
                );
    }

    @ArchTest
    static final ArchRule cross_module_dependencies_should_not_use_user_internals =
            moduleInternalsRule("user");

    @ArchTest
    static final ArchRule cross_module_dependencies_should_not_use_catalog_internals =
            moduleInternalsRule("catalog");

    @ArchTest
    static final ArchRule cross_module_dependencies_should_not_use_inventory_internals =
            moduleInternalsRule("inventory");

    @ArchTest
    static final ArchRule cross_module_dependencies_should_not_use_cart_internals =
            moduleInternalsRule("cart");

    @ArchTest
    static final ArchRule cross_module_dependencies_should_not_use_order_internals =
            moduleInternalsRule("order");

    @ArchTest
    static final ArchRule cross_module_dependencies_should_not_use_payment_internals =
            moduleInternalsRule("payment");

    @ArchTest
    static final ArchRule cross_module_dependencies_should_not_use_outbox_internals =
            moduleInternalsRule("outbox");

    @ArchTest
    static final ArchRule cross_module_dependencies_should_not_use_security_internals =
            moduleInternalsRule("security");

    @ArchTest
    static final ArchRule controllers_should_not_depend_directly_on_repositories =
            noClasses().that().haveSimpleNameEndingWith("Controller")
                    .should().dependOnClassesThat().haveSimpleNameEndingWith("Repository")
                    .orShould().dependOnClassesThat().resideInAPackage("..repository..");
}
