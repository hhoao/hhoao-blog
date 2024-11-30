---
title: Hello, world!
author: John Doe
date: 2021-05-31
complete: true
---
# Flink CEP深入解析

https://developer.aliyun.com/article/738454

在标准的CEP应用中，我们需要创建Pattern、PatternProcessFunction来表示我们需要捕获的事件序列和对捕获的事件序列的处理逻辑，然后通过CEP.pattern(DataStream, Pattern).process(PatternProcessFunction) 操作将 Source、Pattern、PatternProcessFunction联系在一起生成一个CEPOperator算子，当然应用程序中最复杂的还是CEPOperator的创建和CEPOpeartor的运行流程。

CEPOperator在运行过程中最重要的两个变量是NFA（非确定有限自动机）和 SharedBuffer，NFA是操作事件的对象，SharedBuffer用于存储过程中匹配事件。

NFA中设有一个状态(State)集合，它可以理解为由点和边组成的一个状态图，NFA以一个初始状态作为起点，经过一系列的中间状态，达到终态。状态分为**起始状态(Start State)**、**中间状态(Normal State)**、**等待状态(Pending State)**、**最终状态(Final State)、终止状态(Stop State)**三种，边分为**take**、**ignore**、**proceed**三种。

- **take**：必须存在一个条件判断，当到来的消息满足take边条件判断时，把这个消息放入结果集，将状态转移到下一状态。
- **ignore**：当消息到来时，可以忽略这个消息，将状态自旋在当前不变，是一个自己到自己的状态转移。 
- **proceed**：又叫做状态的空转移，当前状态可以不依赖于消息到来而直接转移到下一状态。举个例子，当用户购买商品时，如果购买前有一个咨询客服的行为，需要把咨询客服行为和购买行为两个消息一起放到结果集中向下游输出；如果购买前没有咨询客服的行为，只需把购买行为放到结果集中向下游输出就可以了。 也就是说，如果有咨询客服的行为，就存在咨询客服状态的上的消息保存，如果没有咨询客服的行为，就不存在咨询客服状态的上的消息保存，咨询客服状态是由一条proceed边和下游的购买状态相连。

每个状态中还保存着自己的状态名、上一个状态、下一个状态以及转换到下一个状态的条件，它根据条件和边转化成下一个状态。

接下来我们将从CEPOperator的创建和CEPOpeartor的运行流程入手，深入解析Flink CEP。

![02NFA.png](./Flink CEP深入解析.assets/f202d5b225254cb38bbd63ae7fa38256.png)



## CEPOperator的创建

CEPOpeator 的创建中最复杂的一部分是创建 NFA 中的状态图，它是由NFAFactoryCompiler编译Pattern而成的，下面先介绍一下Pattern。

Pattern保存着上一个Pattern和转化为下一个Pattern的条件，它还有当前Patten匹配次数、循环时的终止条件、匹配后的跳过策略、消费策略、匹配之间完成的最大时间间隔。

Pattern有三种跳过策略、五种消费策略和五种属性，如下：

跳过策略(只有在调用Pattern#begin时可以设置)：

* NoSkipStrategy：不跳过事件
* SkipToNextStrategy：跳到下一个事件
* SkipPastLastStrategy：跳到过去最后一个事件
* SkipToFirstStrategy：跳到第一个事件
* SkipToLastStrategy：跳到最后一个事件

消费策略：

 Pattern：`"a b+ c"`，输入：`"a", "b1", "d1", "b2", "d2", "b3" "c"`

* STRICT(consecutive、begin、next)：表示下一次事件必须是指定事件，匹配 `{a b1 c} 、 {a b2 c} 、 {a b3 c} `
* SKIP_TILL_NEXT(followedBy)：跳过不匹配的事件直到下一个匹配的事件，匹配 `{a b1 c} 、 {a b1 b2 c} 、 {a b1 b2 b3 c} 、 {a b2 c} 、 {a b2 b3 c} 、 {a b3 c} `
* SKIP_TILL_ANY(followedByAny、combinations)：跳过不匹配的事件到任何匹配的事件，匹配 `{a b1 c} 、 {a b1 b2 c} 、 {a b1 b2 b3 c} 、 {a b2 c} 、 {a b2 b3 c} 、 {a b3 c}`
* NOT_FOLLOW(notFollowedBy)：随后的事件中没有指定事件
* NOT_NEXT(notNext)：下一个事件不是指定事件

属性：

* SINGLE：单次的事件
* LOOPING(oneOrMore、times)：重复的事件
* TIMES(times)：指定次数的事件
* OPTIONAL(optional)：可选的事件
* GREEDY(greedy)：贪婪的事件

下面就先介绍 NFAFactoryCompiler 编译 Pattern 的过程。



### NFAFactoryCompiler编译Pattern

* NFAFactoryCompiler编译Pattern

  `NFACompiler#compileFactory`

  * 检测Pattern中所有Pattern的名字是否有重复，如果重复则抛出异常

    `NFAFactoryCompiler#checkPatternNameUniqueness`

  * 检测AfterMatchSkipStrategy

    `NFAFactoryCompiler#checkPatternSkipStrategy`

  * **创建最终状态，并添加到State集合中**

    `NFAFactoryCompiler#createEndingState`

  * **创建中间状态，通过循环调用Pattern#getPrevious获取当前状态的上一个Pattern，获取成功说明获取的Pattern为中间Pattern，然后使用中间Pattern创建中间状态，获取失败则说明获取的Pattern是初始Pattern，初始Pattern将不会用来创建中间状态**

    `NFAFactoryCompiler#createMiddleStates`

    * **循环的创建中间状态的过程：**

        基本每次创建的状态都会以上一次循环创建的状态作为下一个状态，但是第一次循环的状态即是前面步骤中创建的最终状态。

        创建状态的步骤：

        1. 如果Pattern为NOT_FOLLOW策略且满足下面条件，则会创建一个等待状态和一个终止状态，等待状态再添加一条Proceed边，该边的目标状态为刚创建的终止状态，然后等待状态将添加一个Ingore边（Ignore边的目标状态即是自己），Ignore边的匹配条件是当前Pattern的非匹配条件，最后将等待状态作为下次循环的上一个状态。

            条件如下：

            * 匹配范围(WithinType)为PREVIOUS_AND_CURRENT或窗口时间0，
            * 上一个状态为最终状态

        2. 如果Pattern的消费策略为NOT_NEXT，则创建一个中间状态和一个终止状态，如果上次循环的状态是最终状态，则创建一个Ignore边，如果不是，则创建一个Ignore边，上次循环的状态将作为新边所指向的状态。最后中间状态再添加一个Process边，并将创建的终止状态设置为该边的目标状态，最后将中间状态作为下次循环的上一个状态。

        3. 如果Pattern消费策略既不是NOT_NEXT也不是NOT_FOLLOW，则将直接调用convertPattern函数将当前Pattern转化为对应的状态（转换Pattern的操作将在下面说），最后将转换后的状态作为下次循环的上一个状态。

        保存当前Pattern和当前Pattern的前一个Pattern，它们将用于创建状态的步骤中的第三个步骤，如果Pattern设置了窗口时间，则设置全局的窗口时间

  * **创建初始状态，创建中间状态后当前的Pattern一定为初始Pattern，因为创建中间状态的循环中获取不到当前状态的上一个状态了，所以直接转换该初始Pattern为即可获得初始状态，转换Pattern的操作将在后面说。**

    `NFAFactoryCompiler#createStartState`

  * 检测窗口时间是否在全局窗口时间的集合里面

    `NFAFactoryCompiler#checkPatternWindowTimes`



NFACompiler编译Pattern时做的最多的操作是**创建State**和**转换Pattern**这两个操作，下面来剖析这两个操作：



### **转换Pattern操作（convertPattern）**

* **转换Pattern操作（convertPattern）：**

    `NFACompiler#convertPattern`

    先判断Pattern的属性

    * 如果为LOOPING属性（timesOrMore、oneOrMore）

        * 以下一个状态为源状态，复制一个没有NotConditiion的可传递的状态

            `NFACompiler#copyWithoutTransitiveNots`

        * 创建循环状态

            `NFACompiler#createLooping`

    * 如果为TIMES属性，则直接创建Times状态

        `NFACompiler#createTimesState`

    * 否则创建Singleton状态

        `NFACompiler#createSingletonState`

    给转换后的State添加StopState，最后返回转换后的State

* 复制可传输的没有NotCondition的State，步骤如下：

    `NFACompiler#copyWithoutTransitiveNots`

    1. 获取currentPattern的所有NotConditiion

        `NFACompiler#getCurrentNotCondition`

    2. 创建与被复制的State名字相同和类型的State

        `NFACompiler#createState`

    3. 开始复制被复制的State的所有StateTransition

        1. 如果StateTransition的Action类型为PROCEED，



### **创建状态操作**

状态种类有多种，每种状态都有各自的创建流程：

* 通过状态类型和边创建状态

    `NFACompiler#createState`

    该状态创建完后会将Pattern的窗口时间存放到windowTimes的Map集合中并且将该存储到states的List集合中，windowTimes集合以Pattern的名字为key，时间长度为value

* 创建Singleton状态

    `NFACompiler#createSingletonState`

    1. 先获取TaskCondition和IgnoreCondition
    2. 根据当前Pattern创建NormalState
    3. 以下一个State为模板，复制一个可传递的非Not的状态，创举并添加到当前State的Take边上
    4. 如果当前State是可选的
        * 判断当前Pattern是否是贪婪的GREEDY，且有UntilContition，则给当前状态添加一条目标状态为下一个状态的且条件为UntilContition的PROCESS转移边和一条目标状态为下一个状态的且条件为NotUntilContition的PROCESS转移边
        * 则给当前状态添加一条目标状态为下一个状态的无条件的PROCESS空转移边
    5. 如果有IgnoreCondition，则还需进行以下判断
        * 如果当前状态可选，则复制一个以当前状态为模板但是没有PROCESS边的状态，给该复制的状态添加stop边，然后给当前状态添加一条目标状态为这个复制状态且Condition为IgnoreCondition的IGNORED转移边
        * 则给当前状态添加一条目标状态为当前状态且Condition为IgnoreCondition的IGNORED转移边
    6. 返回创建的状态

* 创建Stop状态

    `NFACompiler#createStopState`

    * 如果已经存在Stop状态则直接返回存在的Stop状态
    * 否则创建一个Stop类型且的状态
    * 返回Stop状态

* 创建Looping状态

    `NFACompiler#createLooping`

    * 根据当前Pattern创建普通的状态
    * 判断当前的Pattern的属性是否为GREEDY，需要给当前状态添加既满足TakeCondition又不满足
        * 如果是，且当前的Pattern存在UntilCondition，则给当前状态添加一条目标状态为下一个状态的且条件为UntilContition的PROCESS转移边和一条目标状态为下一个状态的且条件为NotUntilContition的PROCESS转移边，否则给当前状态添加一条目标状态为下一个状态的无条件的PROCESS空转移边
        * 否则则给当前状态添加一条目标状态为下一个状态的无条件的PROCESS空转移边
    * 给当前状态添加目标状态为下一个状态且条件为当前Pattern的TakeCondition的Take边
    * 如果存在Ignored条件，则复制一个以当前状态为模板但是没有PROCESS边的状态，给该复制的状态添加stop边，然后给当前状态添加一条目标状态为这个复制状态且Condition为IgnoreCondition的IGNORED转移边

* 创建Times状态

    `NFACompiler#createTimesState`



### 边的创建

1. 获取TakeCondition：

    如果Pattern的Condition为UntilCondition，则开始判断：

    * 如果有其他的Condition，则创建RichAndCondition
    * 否则直接创建RichAndCondition

    > RichAndCondition: 也就是则在原Condition前封装一层UntilCondition，每次判断Condition时都需要先判断UntilCondition

2. 获取IgnoreCondition

    先获取Pattern的消费策略ConsumingStrategy，根据消费策略创建IgnoreCondition

    1. 如果消费策略为STRICT，则不创建Condition
    2. 如果消费策略为SKIP_TILL_NEXT（followedBy），则创建RichNotCondition
    3. 如果消费策略为SKIP_TILL_ANY（followedByAny），则创建始终返回True的Condition

    如果有UntilCondition，则根据上面创建RichAndCondition





## 开始事件处理

* 配置 CEPOperator

  `CEPOperator#setup`



* 初始化 CEPOpeartor 状态

  `CEPOperator#initializeState`



* 开启算子

  `CEPOperator#open`

  算子open创建了很多运行时需要的组件，如下

  1. 获取算子内部的时间服务
  2. 创建NFA
  3. 创建CEPOperator.ContextFunctionImpl
  4. 创建TimestampedCollector
  5. 创建TimerServiceImpl
  6. 初始化相关指标

* 开始处理事件时，首先要判断是否是处理时间

  `CEPOperator#processElement`

  1. 如果是，判断是否设置了比较器

     1. 如果是，

     2. 否则，则开始给事件添加上处理时间，用于计算一段时间内的有多少事件，并添加到元素队列elementQueueState中

        `CEPOperator#bufferEvent`

  2. 否则，获取元素自带的Timestamp（需要开启Watermark），根据事件自带的timestamp做以下处理：

     1. 如果Timestamp大于当前的Watermark，则直接给事件添加上处理时间，并添加到元素队列elementQueueState中同上

        `CEPOperator#bufferEvent`

     2. 如果设置了OutputTag，则将事件交给Output处理

     3. 否则，将丢弃该事件




* **触发Watermark时将会调用该函数，表示真正开始处理事件，它会进行以下操作：**

  **CEPOperator#onEventTime**

  1. 先将元素队列这一段时间收集的事件取出来，放入以处理时间排序的优先队列中

  2. 获取当前的NFAState，如果没有则会创建并初始化一个NFAState，刚初始化的State只会包含一个Startstate

  3. **对排好序的时间戳进行循环，处理相应时间戳对应的所有事件**

     1. 首先将给定NFAState的时间推进到对应的时间戳中。这意味着不能再将时间戳低于给定时间戳的事件传递给nfa，这可能导致修剪和超时。

     2. **处理根据时间戳获取的事件**

        `CepOperator#processEvent`

        * **NFA处理事件**

          NFA#process -> NFA#doProcess，在这个步骤中间会创建一个EventWrapper，它存储了事件Id、timestamp、记录等信息

          * 遍历NFAState中开始的所有状态

            * 计算下一个State状态

              `NFA#computeNextStates `

              * 创建ConditionContext

              * **创建决策图**，它将判断当前ComputationState的State中所有Transition是否满足Condition，也就是我们自定义的Condition，如果满足条件，则开始将StateTransitionAction为IGNORE和TAKE的Transition添加到outgoingEdges中，如果Action为PROCEED，则将当Transition的State作为下一个判断Transition的State，最后返回outgoingEdges

                `NFA#createDecisionGraph`

              * 分别从outgoingEdges获取总的IGNORE和TAKE分支数量，ignoreBranchesToVisit、takeBranchesToVisit、totalTakeToSkip

              * 开始遍历出边Transition，并根据Transition的Action做出相应处理（基于先前计算的出边创建计算版本，我们需要推迟计算状态的创建，直到我们知道有多少边在这个计算状态开始，这样我们才能分配适当的版本）

                * 如果StateTransitionAction为IGNORE且ComputationState不为StartState，则开始判断目标边的State和当前ComputationState状态是否为等效状态

                   * 如果是，则根据IGNORE和TAKE分支数量计算需要增长的版本大小，计算公式为：

                      `takeBranches == 0 && ignoreBranches == 0 ? 0
                              : ignoreBranches + Math.max(1, takeBranches)`

                      然后从当前ComputationState的版本创建增长后的版本

                   * 否则，创建增加`totalTakeToSkip + ignoreBranchesToVisit`后的版本，再添加一个版本阶段（也就是版本号再加个`.0`（""${version}.0")，然后减去一个ignoreBranchesToVisit数量

                   最后创建计算完版本后的当前ComputationState的ComputationState副本，并添加到resultingComputationStates中

                * 如果StateTransitionAction为TAKE

                  * 在当前的Version的基础上将增加takeBranchesToVisit，并且获取添加一位数后的复制后的版本，然后将takeBranchesToVisit减1

                  * 获取startTimestamp和startEventId

                  * 如果当前的ComputationState为StartState，则以事件中的timestamp、eventId作为startTimestamp，startEventId

                  * 创建一个当前ComputationState不同版本的副本，并添加到resultingComputationStates中，该该副本的状态为当前Transition的目标状态，previousTimestamp为EventWrapper中的timestamp，它的版本、startTimestamp、startEventID是在步骤1和2中获取的

                  * 通过处理Proceed State找最终状态

                    NFA#findFinalStateAfterProceed

                    维护一个栈，先放入当前State，进行循环，推出栈内的State，如果当前State状态转换的Action是PROCESS，则开始**checkFilterCondition**，如果check成功，判断目标状态是否为Final State，如果是则返回，否则将目标状态进行放入栈中，完成循环，如果栈中有元素则会一直循环，如果没有获取到最终状态则返回空

                  * 如果上一步获取到了最终状态，则会将最终状态转换为ComputationState，并添加到resultingComputationStates中，其他属性和步骤二中的一样

                    > ComputationState是封装NFA计算的currentStateName的Helper类。它指向当前的currentStateName、模式的前一个条目、当前版本和整个模式的起始时间戳。

              * 如果ComputationState为Start State

                则创建一个增加了`ignoreBranches + Math.max(1, takeBranches)`的版本号的Start ComputationState，然后添加到resultingComputationStates中

              * 返回resultingComputationStates

            * 如果获取到的下一个状态数量不为1或下一个状态不为当前State，则设置NFAState状态改变

            * 处理多条下一次状态

          * 遍历计算的多条下一次状态，根据状态的类型做出相应的操作：

            1. 如果下一条状态为Start状态，且StartTimestamp大于0，则将NFAState的isNewStartPartialMatch设置为true
            2. 如果为Final状态，则将该状态添加到完成的状态队列中
            3. 如果为Stop状态，则释放该状态
            4. 其他的，添加该状态到保留的状态队列中，它将在下一个事件到达时进行处理

          * 遍历完所有的下一次状态后后，如果下一次状态中有Stop状态，则释放当前状态链路所有状态，否则添加所有保留的下一次状态到新状态当中，它将在下一个事件到达时进行处理

        * 根据AfterMatchSkipStrategy处理匹配的事件序列

          `NFA#processMatchesAccordingToSkipStrategy`

          将完成的下一次状态添加到NFAState的completedMatches队列中，然后开始遍历完成的状态：

          * 如果AfterMatchSkipStrategy为跳过策略
          * 

     3. 判断NFA是否开启WindowTime和NFAState是否为StartState，如果是则根据WindowTime注册计时器

     4. **处理完成匹配的Event事件序列**

        `CepOperator#processMatchedSequences`

        * 获取自定义的PatternProcessFunction，设置CepOperator. ContextFunctionImpl处理时间为此次事件处理的timestamp，然后以匹配的事件序列、CepOperator. ContextFunctionImpl、TimestampedCollector作为参数，调用自定义的PatternProcessFunction开始处理匹配的Event事件序列

  4. 将时间提前到当前水位线，以便丢弃过期的Pattern，将给定NFA的时间推进到给定时间戳。这意味着不能再将时间戳低于给定时间戳的事件传递给nfa，这可能导致修剪和超时。

  5. 更新NFA



* CEPOperator#onProcessingTime



> NFAState由partialMatches和completedMatches组成，它在初始化时会将Start State放入其中



> Dewey数
>
> Dewey数由一串数字d1.d2.d3. ... .dn组成。Dewey数v与v'兼容，前提是v包含v'作为前缀，或者两个杜威数只有最后一位不同且v的最后一位大于v'。



SharedBufferAccessor用于访问SharedBuffer

SharedBuffer用于存储匹配的事件记录，每个事件记录对应一个EventId，事件记录由SharedBuffer的`Cache<EventId, Lockable<V>> eventsBufferCache`变量存储

每一个事件都有一个EventId

