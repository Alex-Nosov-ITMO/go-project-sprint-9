package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

var mu sync.Mutex

// Generator генерирует последовательность чисел 1,2,3 и т.д. и
// отправляет их в канал ch. При этом после записи в канал для каждого числа
// вызывается функция fn. Она служит для подсчёта количества и суммы
// сгенерированных чисел.
func Generator(ctx context.Context, ch chan<- int64, fn func(int64)) {
	defer close(ch) // не забываем закрыть канал перед выходом
	var n int64 = 1
	for {
		select {
		case <-ctx.Done():
			return
		default:
			ch <- n
			fn(n)
			n++
		}
	}
}

// Worker читает число из канала in и пишет его в канал out.
func Worker(in <-chan int64, out chan<- int64) {
	defer close(out) // не забываем закрыть канал по окончанию функции

	for v := range in { // работает, пока не закроется канал in
		out <- v
		time.Sleep(1 * time.Millisecond)
	}
}

func main() {
	chIn := make(chan int64)

	// Контекст с TimeOut
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel() // обязательно вызываем функцию cancel, чтобы сборщик мусора освободил память

	var inputSum atomic.Int64   // сумма сгенерированных чисел
	var inputCount atomic.Int64 // количество сгенерированных чисел

	// генерируем числа, считая параллельно их количество и сумму
	go Generator(ctx, chIn, func(i int64) {
		inputCount.Add(1) // исспользуем потокобезопасные операции, так как эти поля изменяют несколько горутин
		inputSum.Add(i)
	})

	const NumOut = 20 // количество обрабатывающих горутин и каналов

	// outs — слайс каналов, куда будут записываться числа из chIn
	outs := make([]chan int64, NumOut)
	for i := 0; i < NumOut; i++ {
		// создаём каналы и для каждого из них вызываем горутину Worker
		outs[i] = make(chan int64)

		go Worker(chIn, outs[i])
	}

	// amounts — слайс, в который собирается статистика по горутинам
	amounts := make([]int64, NumOut)
	// chOut — канал, в который будут отправляться числа из горутин `outs[i]`
	chOut := make(chan int64, NumOut)

	var wg sync.WaitGroup // создаем переменную типа sync.WaitGroup, чтобы дождаться завершения всех горутин

	// 4. Собираем числа из каналов outs
	for i, out := range outs {
		wg.Add(1)
		go func(i int, ch <-chan int64) {
			defer wg.Done()
			for v := range ch {
				mu.Lock() // исспользуем мютекс, чтобы обезопасить изменение данных
				amounts[i]++
				mu.Unlock()
				chOut <- v
			}
		}(i, out)
	}

	go func() {
		// ждём завершения работы всех горутин для outs
		wg.Wait()
		// закрываем результирующий канал
		close(chOut)
	}()

	var count atomic.Int64 // количество чисел результирующего канала
	var sum atomic.Int64   // сумма чисел результирующего канала

	// 5. Читаем числа из результирующего канала

	var wg1 sync.WaitGroup
	wg1.Add(1)
	go func(ch <-chan int64) {
		defer wg1.Done()
		for v := range ch {
			count.Add(1) // также используем потокобезопасные типы данных
			sum.Add(v)
		}
	}(chOut)
	wg1.Wait()

	fmt.Println("Количество чисел", inputCount.Load(), count.Load())
	fmt.Println("Сумма чисел", inputSum.Load(), sum.Load())
	fmt.Println("Разбивка по каналам", amounts)

	// проверка результатов
	if inputSum.Load() != sum.Load() {
		log.Fatalf("Ошибка: суммы чисел не равны: %d != %d\n", inputSum.Load(), sum.Load())
	}
	if inputCount.Load() != count.Load() {
		log.Fatalf("Ошибка: количество чисел не равно: %d != %d\n", inputCount.Load(), count.Load())
	}
	for _, v := range amounts {
		inputCount.Add(-v)
	}
	if inputCount.Load() != 0 {
		log.Fatalf("Ошибка: разделение чисел по каналам неверное\n")
	}
}
