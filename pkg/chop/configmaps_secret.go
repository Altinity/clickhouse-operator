package chop

import (
	"context"
	"log"
	"os"

	corev1 "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kube "k8s.io/client-go/kubernetes"
)

var cmNamePaths = map[string]string{
	"etc-clickhouse-operator-files":            "/etc/clickhouse-operator/",
	"etc-clickhouse-operator-confd-files":      "/etc/clickhouse-operator/conf.d/",
	"etc-clickhouse-operator-configd-files":    "/etc/clickhouse-operator/config.d/",
	"etc-clickhouse-operator-templatesd-files": "/etc/clickhouse-operator/templates.d/",
	"etc-clickhouse-operator-usersd-files":     "/etc/clickhouse-operator/users.d/",
}

func HandleConfigmapsCreation(kubeClient *kube.Clientset) error {
	var cm_namespace = os.Getenv("POD_NAMESPACE")

	for name, path := range cmNamePaths {
		_, err := kubeClient.CoreV1().ConfigMaps(cm_namespace).Get(context.TODO(), name, metav1.GetOptions{})

		if apiErrors.IsNotFound(err) {
			cm := makeConfigmap(name, path)
			_, err := kubeClient.CoreV1().ConfigMaps(cm.Namespace).Create(context.TODO(), cm, metav1.CreateOptions{})
			if err != nil {
				log.Printf("Creating ConfigMap %s/%s failed with error %v", cm.Namespace, cm.Name, err)
				return err
			}
		} else if err != nil {
			log.Printf("Error probing ConfigMap %s/%s with error %v", cm_namespace, name, err)
		} else {
			log.Printf("ConfigMap %s/%s already exists", cm_namespace, name)
		}
	}
	return nil
}

func makeConfigmap(name string, path string) *corev1.ConfigMap {
	configs := getConfigsFrom(path)

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: os.Getenv("POD_NAMESPACE"),
		},
		Data: configs,
	}
	return cm
}

func getConfigsFrom(path string) map[string]string {
	configs := map[string]string{}

	files, err := os.ReadDir(path)
	if err != nil {
		log.Fatal(err)
	}

	for _, f := range files {
		if !f.IsDir() && f.Name() != ".gitkeep.xml" {
			dat, err := os.ReadFile(path + f.Name())
			if err != nil {
				log.Fatal(err.Error())
			}
			configs[f.Name()] = string(dat)
		}
	}

	return configs
}

func HandleSecretCreation(kubeClient *kube.Clientset) error {
	const secret_name = "clickhouse-operator"
	var secret_namespace = os.Getenv("POD_NAMESPACE")

	_, err := kubeClient.CoreV1().Secrets(secret_namespace).Get(context.TODO(), secret_name, metav1.GetOptions{})

	if apiErrors.IsNotFound(err) {
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: secret_namespace,
				Name:      secret_name,
			},
			StringData: map[string]string{
				"username": "clickhouse_operator",
				"password": "clickhouse_operator_password",
			},
			Type: corev1.SecretTypeOpaque,
		}

		_, err := kubeClient.CoreV1().Secrets(secret_namespace).Create(context.TODO(), secret, metav1.CreateOptions{})
		if err != nil {
			log.Printf("Creating Secret %s/%s failed with error %v", secret.Namespace, secret.Name, err)
			return err
		}
	} else if err != nil {
		log.Printf("Error probing Secret %s/%s with error %v", secret_namespace, secret_name, err)
		return err
	} else {
		log.Printf("Secret %s/%s already exists", secret_namespace, secret_name)
	}
	return nil
}
